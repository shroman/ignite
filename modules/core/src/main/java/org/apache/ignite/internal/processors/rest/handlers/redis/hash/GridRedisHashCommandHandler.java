/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.rest.handlers.redis.hash;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.rest.GridRestProtocolHandler;
import org.apache.ignite.internal.processors.rest.handlers.redis.GridRedisRestCommandHandler;
import org.apache.ignite.internal.processors.rest.handlers.redis.exception.GridRedisGenericException;
import org.apache.ignite.internal.processors.rest.protocols.tcp.redis.GridRedisMessage;
import org.apache.ignite.internal.processors.rest.request.GridRestCacheRequest;
import org.apache.ignite.internal.processors.rest.request.GridRestRequest;

/**
 * Hash base class.
 */
abstract class GridRedisHashCommandHandler extends GridRedisRestCommandHandler {
    /** Grid context. */
    private final GridKernalContext ctx;

    /**
     * Handler constructor.
     *
     * @param log Logger to use.
     * @param hnd Rest handler.
     * @param ctx Context.
     */
    GridRedisHashCommandHandler(final IgniteLogger log, final GridRestProtocolHandler hnd,
        GridKernalContext ctx) {
        super(log, hnd);

        this.ctx = ctx;
    }

    /** {@inheritDoc} */
    @Override public GridRestRequest asRestRequest(GridRedisMessage msg) throws IgniteCheckedException {
        assert msg != null;

        if (msg.messageSize() < 2)
            throw new GridRedisGenericException("Wrong number of arguments");

        // make sure cache exists.
        String cacheName = GridRedisMessage.CACHE_NAME_PREFIX + "-" + msg.key();

        CacheConfiguration ccfg = ctx.cache().cacheConfiguration(GridRedisMessage.DFLT_CACHE_NAME);
        ccfg.setName(cacheName);

        ctx.grid().getOrCreateCache(ccfg);

        GridRestCacheRequest restReq = new GridRestCacheRequest();

        restReq.clientId(msg.clientId());
        restReq.cacheName(cacheName);

        return handle(msg, restReq);
    }

    /**
     * Handles hash command-specific logic.
     *
     * @param msg {@link GridRedisMessage}
     * @param restReq {@link GridRestCacheRequest}
     * @return {@link GridRestRequest}
     * @throws IgniteCheckedException
     */
    protected abstract GridRestRequest handle(GridRedisMessage msg,
        final GridRestCacheRequest restReq) throws IgniteCheckedException;
}
