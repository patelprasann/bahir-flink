/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.streaming.connectors.mongodb;

import com.mongodb.MongoClientSettings;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.util.Preconditions;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

/**
 * @param <OUT> The type of the data read from MongoDB.
 */
public class MongoDBSource<OUT>
        extends RichSourceFunction<OUT> {
    private static final long serialVersionUID = -8689291992192955579L;
    private static final Logger LOG = LoggerFactory.getLogger(MongoDBSource.class);

    private final Class<OUT> klass;
    private final MongoDBConfig mongoDBConfig;
    private final MongoClient mongoClient;
    private SubscriberHelpers.ObservableSubscriber<OUT> subscriber;

    public MongoDBSource(MongoDBConfig mongoDBConfig,
                         Class<OUT> klass) {
        this.mongoDBConfig = Preconditions.checkNotNull(mongoDBConfig,
                "MongoDB client config should not be null");
        this.klass = Preconditions.checkNotNull(klass,
                "Type of POJO should not be null");

        CodecRegistry pojoCodecRegistry = fromRegistries(
                MongoClientSettings.getDefaultCodecRegistry(),
                fromProviders(PojoCodecProvider.builder().automatic(true).build()));
        MongoClientSettings settings = MongoClientSettings
                .builder()
                .codecRegistry(pojoCodecRegistry)
                .build();
        mongoClient = MongoClients.create(settings);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        try {
            MongoDatabase database = mongoClient.getDatabase(mongoDBConfig.getDatabaseName());
            MongoCollection<OUT> collection = database.getCollection(
                    mongoDBConfig.getCollectionName(),
                    this.klass);
            collection
                    .find(mongoDBConfig.getFilter())
                    .projection(mongoDBConfig.getProjection())
                    .sort(mongoDBConfig.getSort())
                    .subscribe(subscriber);
        } catch (Exception e) {
            LOG.error("Error: " + e, e);
        }
    }

    @Override
    public void close() throws Exception {
        super.close();

        subscriber.getSubscription().cancel();
        mongoClient.close();
    }

    @Override
    public void run(SourceContext<OUT> sourceContext) {
        try {
            List<OUT> outList = subscriber.get(
                    mongoDBConfig.getRequestTimeout(),
                    TimeUnit.MILLISECONDS);

            outList.forEach(sourceContext::collect);
        } catch (Throwable e) {
            LOG.error("Error: " + e, e);
        }
    }

    @Override
    public void cancel() {
        subscriber.getSubscription().cancel();
    }
}
