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

import com.mongodb.ConnectionString;
import com.mongodb.MongoCredential;
import org.apache.flink.util.Preconditions;
import org.bson.conversions.Bson;
import software.amazon.awssdk.regions.Region;

import java.io.Serializable;

public class MongoDBConfig implements Serializable {
    private static final long serialVersionUID = -8689291992192955579L;

    private static final int DEFAULT_MAX_CONNECTIONS = 5_000;
    private static final int DEFAULT_CONNECTION_TIMEOUT = 20_000;
    private static final int DEFAULT_REQUEST_TIMEOUT = 20_000;
    private static final int DEFAULT_MAX_ERROR_RETRY = 10;
    private static final Region DEFAULT_REGION = Region.US_EAST_1;

    private final int maxConnections;
    private final int connectionTimeout;
    private final int requestTimeout;
    private final int maxErrorRetryLimit;
    private final String applicationName;
    private final ConnectionString connectionString;
    private final MongoCredential credential;
    private final Region region;
    private final String databaseName;
    private final String collectionName;
    private final Bson filter;
    private final Bson projection;
    private final Bson sort;

    public MongoDBConfig(MongoDBConfig.Builder builder) {
        Preconditions.checkArgument(builder != null, "MongoDBConfig builder can not be null");
        Preconditions.checkNotNull(builder.getMaxConnections(), "Max connections cannot be null");
        Preconditions.checkNotNull(builder.getConnectionTimeout(), "Connection timeout cannot be null");
        Preconditions.checkNotNull(builder.getRequestTimeout(), "Request timeout cannot be null");
        Preconditions.checkNotNull(builder.getMaxErrorRetryLimit(), "Max error retry limit cannot be null");
        Preconditions.checkNotNull(builder.getApplicationName(), "Application name cannot be null");
        Preconditions.checkNotNull(builder.getConnectionString(), "Connection string cannot be null");
        Preconditions.checkNotNull(builder.getCredential(), "Credential cannot be null");
        Preconditions.checkNotNull(builder.getRegion(), "Region cannot be null");
        Preconditions.checkNotNull(builder.getDatabaseName(), "Database name cannot be null");
        Preconditions.checkNotNull(builder.getCollectionName(), "Collection name cannot be null");
        Preconditions.checkNotNull(builder.getFilter(), "Filters cannot be null");
        Preconditions.checkNotNull(builder.getProjection(), "Projections cannot be null");
        Preconditions.checkNotNull(builder.getSort(), "Sort cannot be null");

        this.maxConnections = builder.getMaxConnections();
        this.connectionTimeout = builder.getConnectionTimeout();
        this.requestTimeout = builder.getRequestTimeout();
        this.maxErrorRetryLimit = builder.getMaxErrorRetryLimit();
        this.applicationName = builder.getApplicationName();
        this.connectionString = builder.getConnectionString();
        this.credential = builder.getCredential();
        this.region = builder.getRegion();
        this.databaseName = builder.getDatabaseName();
        this.collectionName = builder.getCollectionName();
        this.filter = builder.getFilter();
        this.projection = builder.getProjection();
        this.sort = builder.getSort();
    }

    public int getMaxConnections() {
        return maxConnections;
    }

    public int getConnectionTimeout() {
        return connectionTimeout;
    }

    public int getRequestTimeout() {
        return requestTimeout;
    }

    public int getMaxErrorRetryLimit() {
        return maxErrorRetryLimit;
    }

    public String getApplicationName() {
        return applicationName;
    }

    public ConnectionString getConnectionString() {
        return connectionString;
    }

    public MongoCredential getCredential() {
        return credential;
    }

    public Region getRegion() {
        return region;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public String getCollectionName() {
        return collectionName;
    }

    public Bson getFilter() {
        return filter;
    }

    public Bson getProjection() {
        return projection;
    }

    public Bson getSort() {
        return sort;
    }

    /**
     * A builder used to create an instance of a MongoDBConfig.
     */
    public static class Builder {
        private int maxConnections = DEFAULT_MAX_CONNECTIONS;
        private int connectionTimeout = DEFAULT_CONNECTION_TIMEOUT;
        private int requestTimeout = DEFAULT_REQUEST_TIMEOUT;
        private int maxErrorRetryLimit = DEFAULT_MAX_ERROR_RETRY;
        private Region region = DEFAULT_REGION;
        private ConnectionString connectionString;
        private MongoCredential credential;
        private String applicationName;
        private String databaseName;
        private String collectionName;
        private Bson filter;
        private Bson projection;
        private Bson sort;

        public Builder() {
        }

        public Builder(Region region,
                       int maxConnections,
                       int connectionTimeout,
                       int requestTimeout,
                       int maxErrorRetryLimit,
                       String applicationName,
                       ConnectionString connectionString,
                       MongoCredential credential,
                       String databaseName,
                       String collectionName,
                       Bson filter,
                       Bson projection,
                       Bson sort) {
            this.region = region;
            this.maxConnections = maxConnections;
            this.connectionTimeout = connectionTimeout;
            this.requestTimeout = requestTimeout;
            this.maxErrorRetryLimit = maxErrorRetryLimit;
            this.applicationName = applicationName;
            this.connectionString = connectionString;
            this.credential = credential;
            this.databaseName = databaseName;
            this.collectionName = collectionName;
            this.filter = filter;
            this.projection = projection;
            this.sort = sort;
        }

        public MongoDBConfig.Builder maxConnections(int maxConnections) {
            this.maxConnections = maxConnections;
            return this;
        }

        public MongoDBConfig.Builder connectionTimeout(int connectionTimeout) {
            this.connectionTimeout = connectionTimeout;
            return this;
        }

        public MongoDBConfig.Builder requestTimeout(int requestTimeout) {
            this.requestTimeout = requestTimeout;
            return this;
        }

        public MongoDBConfig.Builder maxErrorRetryLimit(int maxErrorRetryLimit) {
            this.maxErrorRetryLimit = maxErrorRetryLimit;
            return this;
        }

        public MongoDBConfig.Builder region(Region region) {
            this.region = region;
            return this;
        }

        public MongoDBConfig.Builder applicationName(String applicationName) {
            this.applicationName = applicationName;
            return this;
        }

        public MongoDBConfig.Builder connectionString(ConnectionString connectionString) {
            this.connectionString = connectionString;
            return this;
        }

        public MongoDBConfig.Builder credential(MongoCredential credential) {
            this.credential = credential;
            return this;
        }

        public MongoDBConfig.Builder databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        public MongoDBConfig.Builder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        public MongoDBConfig.Builder filter(Bson filter) {
            this.filter = filter;
            return this;
        }

        public MongoDBConfig.Builder projection(Bson projection) {
            this.projection = projection;
            return this;
        }

        public MongoDBConfig.Builder sort(Bson sort) {
            this.sort = sort;
            return this;
        }

        public MongoDBConfig build() {
            return new MongoDBConfig(this);
        }

        public int getMaxConnections() {
            return this.maxConnections;
        }

        public int getConnectionTimeout() {
            return this.connectionTimeout;
        }

        public int getRequestTimeout() {
            return this.requestTimeout;
        }

        public int getMaxErrorRetryLimit() {
            return this.maxErrorRetryLimit;
        }

        public String getApplicationName() {
            return this.applicationName;
        }

        public ConnectionString getConnectionString() {
            return this.connectionString;
        }

        public MongoCredential getCredential() {
            return this.credential;
        }

        public String getDatabaseName() {
            return this.databaseName;
        }

        public String getCollectionName() {
            return this.collectionName;
        }

        public Bson getFilter() {
            return this.filter;
        }

        public Bson getProjection() {
            return this.projection;
        }

        public Bson getSort() {
            return this.sort;
        }

        public Region getRegion() {
            return this.region;
        }
    }
}
