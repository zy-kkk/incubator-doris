// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.udf;

import org.apache.doris.catalog.Type;
import org.apache.doris.common.Pair;
import org.apache.doris.common.classloader.ScannerLoader;
import org.apache.doris.common.exception.InternalException;
import org.apache.doris.common.exception.UdfRuntimeException;
import org.apache.doris.common.jni.utils.JavaUdfDataType;
import org.apache.doris.common.jni.utils.UdfClassCache;
import org.apache.doris.common.jni.utils.UdfUtils;
import org.apache.doris.common.jni.vec.VectorTable;
import org.apache.doris.thrift.TJavaUdfExecutorCtorParams;

import com.esotericsoftware.reflectasm.MethodAccess;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.apache.log4j.Logger;

import java.io.FileNotFoundException;
import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Map;

public class UdfExecutor extends BaseExecutor {
    public static final Logger LOG = Logger.getLogger(UdfExecutor.class);
    private static final String UDF_PREPARE_FUNCTION_NAME = "prepare";
    private static final String UDF_FUNCTION_NAME = "evaluate";

    // setup by init() and cleared by close()
    private Method method;

    private int evaluateIndex;

    private boolean isStaticLoad = false;

    /**
     * Create a UdfExecutor, using parameters from a serialized thrift object. Used by
     * the backend.
     */
    public UdfExecutor(byte[] thriftParams) throws Exception {
        super(thriftParams);
    }

    /**
     * Close the class loader we may have created.
     */
    @Override
    public void close() {
        // We are now un-usable (because the class loader has been
        // closed), so null out method_ and classLoader_.
        method = null;
        if (!isStaticLoad) {
            super.close();
        } else if (outputTable != null) {
            outputTable.close();
        }
    }

    public long evaluate(Map<String, String> inputParams, Map<String, String> outputParams) throws UdfRuntimeException {
        try {
            VectorTable inputTable = VectorTable.createReadableTable(inputParams);
            int numRows = inputTable.getNumRows();
            int numColumns = inputTable.getNumColumns();
            if (outputTable != null) {
                outputTable.close();
            }
            outputTable = VectorTable.createWritableTable(outputParams, numRows);

            // If the return type is primitive, we can't cast the array of primitive type as array of Object,
            // so we have to new its wrapped Object.
            Object[] result = outputTable.getColumnType(0).isPrimitive()
                    ? outputTable.getColumn(0).newObjectContainerArray(numRows)
                    : (Object[]) Array.newInstance(method.getReturnType(), numRows);
            Object[][] inputs = inputTable.getMaterializedData(getInputConverters(numColumns, false));
            Object[] parameters = new Object[numColumns];
            for (int i = 0; i < numRows; ++i) {
                for (int j = 0; j < numColumns; ++j) {
                    int row = inputTable.isConstColumn(j) ? 0 : i;
                    parameters[j] = inputs[j][row];
                }
                result[i] = methodAccess.invoke(udf, evaluateIndex, parameters);
            }
            boolean isNullable = Boolean.parseBoolean(outputParams.getOrDefault("is_nullable", "true"));
            outputTable.appendData(0, result, getOutputConverter(), isNullable);
            return outputTable.getMetaAddress();
        } catch (Exception e) {
            LOG.warn("evaluate exception: " + debugString(), e);
            throw new UdfRuntimeException("UDF failed to evaluate", e);
        }
    }

    public Method getMethod() {
        return method;
    }

    private Method findPrepareMethod(Method[] methods) {
        for (Method method : methods) {
            if (method.getName().equals(UDF_PREPARE_FUNCTION_NAME) && method.getReturnType().equals(void.class)
                    && method.getParameterCount() == 0) {
                return method;
            }
        }
        return null; // Method not found
    }

    public UdfClassCache getClassCache(String className, String jarPath, String signature, long expirationTime,
            Type funcRetType, Type... parameterTypes)
            throws MalformedURLException, FileNotFoundException, ClassNotFoundException, InternalException,
            UdfRuntimeException {
        UdfClassCache cache = null;
        if (isStaticLoad) {
            cache = ScannerLoader.getUdfClassLoader(signature);
        }
        if (cache == null) {
            ClassLoader loader;
            if (Strings.isNullOrEmpty(jarPath)) {
                // if jarPath is empty, which means the UDF jar is located in custom_lib
                // and already be loaded when BE start.
                // so here we use system class loader to load UDF class.
                loader = ClassLoader.getSystemClassLoader();
            } else {
                ClassLoader parent = getClass().getClassLoader();
                classLoader = UdfUtils.getClassLoader(jarPath, parent);
                loader = classLoader;
            }
            cache = new UdfClassCache();
            cache.udfClass = Class.forName(className, true, loader);
            cache.methodAccess = MethodAccess.get(cache.udfClass);
            checkAndCacheUdfClass(className, cache, funcRetType, parameterTypes);
            if (isStaticLoad) {
                ScannerLoader.cacheClassLoader(signature, cache, expirationTime);
            }
        }
        return cache;
    }

    private void checkAndCacheUdfClass(String className, UdfClassCache cache, Type funcRetType, Type... parameterTypes)
            throws InternalException, UdfRuntimeException {
        ArrayList<String> signatures = Lists.newArrayList();
        Class<?> c = cache.udfClass;
        Method[] methods = c.getMethods();
        Method prepareMethod = findPrepareMethod(methods);
        if (prepareMethod != null) {
            cache.prepareMethod = prepareMethod;
        }
        for (Method m : methods) {
            // By convention, the udf must contain the function "evaluate"
            if (!m.getName().equals(UDF_FUNCTION_NAME)) {
                continue;
            }
            signatures.add(m.toGenericString());
            cache.argClass = m.getParameterTypes();

            // Try to match the arguments
            if (cache.argClass.length != parameterTypes.length) {
                continue;
            }
            cache.method = m;
            cache.evaluateIndex = cache.methodAccess.getIndex(UDF_FUNCTION_NAME, cache.argClass);
            Pair<Boolean, JavaUdfDataType> returnType;
            if (cache.argClass.length == 0 && parameterTypes.length == 0) {
                // Special case where the UDF doesn't take any input args
                returnType = UdfUtils.setReturnType(funcRetType, m.getReturnType());
                if (!returnType.first) {
                    continue;
                } else {
                    cache.retType = returnType.second;
                }
                cache.argTypes = new JavaUdfDataType[0];
                return;
            }
            returnType = UdfUtils.setReturnType(funcRetType, m.getReturnType());
            if (!returnType.first) {
                continue;
            } else {
                cache.retType = returnType.second;
            }
            Pair<Boolean, JavaUdfDataType[]> inputType = UdfUtils.setArgTypes(parameterTypes, cache.argClass, false);
            if (!inputType.first) {
                continue;
            } else {
                cache.argTypes = inputType.second;
            }
            if (cache.method != null) {
                cache.retClass = cache.method.getReturnType();
            }
            return;
        }
        StringBuilder sb = new StringBuilder();
        sb.append("Unable to find evaluate function with the correct signature: ")
                         .append(className)
                         .append(".evaluate(")
                         .append(Joiner.on(", ").join(parameterTypes))
                         .append(")\n")
                         .append("UDF contains: \n    ")
                         .append(Joiner.on("\n    ").join(signatures));
        throw new UdfRuntimeException(sb.toString());
    }

    // Preallocate the input objects that will be passed to the underlying UDF.
    // These objects are allocated once and reused across calls to evaluate()
    @Override
    protected void init(TJavaUdfExecutorCtorParams request, String jarPath, Type funcRetType,
            Type... parameterTypes) throws UdfRuntimeException {
        String className = request.fn.scalar_fn.symbol;
        try {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Loading UDF '" + className + "' from " + jarPath);
            }
            isStaticLoad = request.getFn().isSetIsStaticLoad() && request.getFn().is_static_load;
            long expirationTime = 360L; // default is 6 hours
            if (request.getFn().isSetExpirationTime()) {
                expirationTime = request.getFn().getExpirationTime();
            }
            UdfClassCache cache = getClassCache(className, jarPath, request.getFn().getSignature(), expirationTime,
                    funcRetType, parameterTypes);
            methodAccess = cache.methodAccess;
            Constructor<?> ctor = cache.udfClass.getConstructor();
            udf = ctor.newInstance();
            Method prepareMethod = cache.prepareMethod;
            if (prepareMethod != null) {
                prepareMethod.invoke(udf);
            }

            argClass = cache.argClass;
            method = cache.method;
            evaluateIndex = cache.evaluateIndex;
            retType = cache.retType;
            argTypes = cache.argTypes;
            retClass = cache.retClass;
        } catch (MalformedURLException e) {
            throw new UdfRuntimeException("Unable to load jar.", e);
        } catch (SecurityException e) {
            throw new UdfRuntimeException("Unable to load function.", e);
        } catch (ClassNotFoundException e) {
            throw new UdfRuntimeException("Unable to find class.", e);
        } catch (NoSuchMethodException e) {
            throw new UdfRuntimeException(
                    "Unable to find constructor with no arguments.", e);
        } catch (IllegalArgumentException e) {
            throw new UdfRuntimeException(
                    "Unable to call UDF constructor with no arguments.", e);
        } catch (Exception e) {
            throw new UdfRuntimeException("Unable to call create UDF instance.", e);
        }
    }
}


