/*
 * Copyright (c) 2021 Oracle and/or its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package io.helidon.microprofile.messaging.hook;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.eclipse.microprofile.reactive.streams.operators.ProcessorBuilder;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;

public class GeneratorNewTest extends AbstractHookTest {

    Queue<String> queue = new LinkedList<>(TEST_DATA);

    @Outgoing("channel1")
    @MockLRA(MockLRA.Type.NEW)
    public String publish() {
        return queue.poll();
    }

    @Incoming("channel1")
    @Outgoing("channel2")
    @MockLRA(MockLRA.Type.REQUIRED)
    public ProcessorBuilder<Message<String>, Message<String>> process() {
        return ReactiveStreams.<Message<String>>builder()
                .limit(TEST_DATA.size());
    }

    @Incoming("channel2")
    @MockLRA(MockLRA.Type.REQUIRED)
    public CompletionStage<Void> consume(Message<String> msg) {
        addActual(msg);
        return CompletableFuture.completedStage(null);
    }

}
