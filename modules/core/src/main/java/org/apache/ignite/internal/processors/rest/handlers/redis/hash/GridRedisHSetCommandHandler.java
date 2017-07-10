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

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.rest.GridRestProtocolHandler;
import org.apache.ignite.internal.processors.rest.GridRestResponse;
import org.apache.ignite.internal.processors.rest.handlers.redis.exception.GridRedisGenericException;
import org.apache.ignite.internal.processors.rest.protocols.tcp.redis.GridRedisCommand;
import org.apache.ignite.internal.processors.rest.protocols.tcp.redis.GridRedisMessage;
import org.apache.ignite.internal.processors.rest.protocols.tcp.redis.GridRedisProtocolParser;
import org.apache.ignite.internal.processors.rest.request.GridRestCacheRequest;
import org.apache.ignite.internal.processors.rest.request.GridRestRequest;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.processors.rest.GridRestCommand.CACHE_GET_AND_PUT;
import static org.apache.ignite.internal.processors.rest.protocols.tcp.redis.GridRedisCommand.HSET;

/**
 * Redis HSET command handler.
 */
public class GridRedisHSetCommandHandler extends GridRedisHashCommandHandler {
    /** Supported commands. */
    private static final Collection<GridRedisCommand> SUPPORTED_COMMANDS = U.sealList(
        HSET
    );

    /** Field position in Redis message. */
    private static final int FIELD_POS = 2;

    /** Value position in Redis message. */
    private static final int VAL_POS = 3;

    /**
     * Handler constructor.
     *
     * @param log Logger to use.
     * @param hnd Rest handler.
     * @param ctx Context.
     */
    public GridRedisHSetCommandHandler(final IgniteLogger log, final GridRestProtocolHandler hnd,
        GridKernalContext ctx) {
        super(log, hnd, ctx);
    }

    /** {@inheritDoc} */
    @Override public Collection<GridRedisCommand> supportedCommands() {
        return SUPPORTED_COMMANDS;
    }

    /** {@inheritDoc} */
    @Override public GridRestRequest handle(GridRedisMessage msg,
        final GridRestCacheRequest restReq) throws IgniteCheckedException {
        assert msg != null;

        if (msg.messageSize() < 4)
            throw new GridRedisGenericException("Wrong number of arguments");

        restReq.command(CACHE_GET_AND_PUT);
        restReq.key(msg.aux(FIELD_POS));
        restReq.value(msg.aux(VAL_POS));

        return restReq;
    }

    /** {@inheritDoc} */
    @Override public ByteBuffer makeResponse(final GridRestResponse restRes, List<String> params) {
        Object resp = restRes.getResponse();

        if (resp == null)
            return GridRedisProtocolParser.toInteger(String.valueOf("1"));
        else
            return GridRedisProtocolParser.toInteger(String.valueOf("0"));
    }
}
