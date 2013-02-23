package com.netflix.simianarmy.client.edda;

import com.google.common.collect.Sets;
import com.netflix.simianarmy.MonkeyConfiguration;
import com.netflix.simianarmy.client.MonkeyRestClient;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.Validate;
import org.codehaus.jackson.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

/**
 * The REST client to access Edda to get the history of a cloud resource.
 */
public class EddaClient extends MonkeyRestClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(EddaClient.class);

    private final MonkeyConfiguration config;
    /**
     * Constructor.
     * @param timeout the timeout in miliseconds
     * @param maxRetries the max number of retries
     * @param retryInterval the interval in milisends between retries
     * @param config the monkey configuration
     */
    public EddaClient(int timeout, int maxRetries, int retryInterval, MonkeyConfiguration config) {
        super(timeout, maxRetries, retryInterval);
        this.config = config;
    }

    /**
     * Gets all existing instance ids in a region.
     * @param region the region
     * @return the ids of existing instances
     */
    public Collection<String> getExistingInstanceIds(String region) {
        return getExistingIdsForResourceType(region, "view", "instances");
    }

    /**
     * Gets all existing volume ids in a region.
     * @param region the region
     * @return the ids of existing volumes
     */
    public Collection<String> getExistingVolumeIds(String region) {
        return getExistingIdsForResourceType(region, "aws", "volumes");
    }

    private Collection<String> getExistingIdsForResourceType(String region, String category, String resourceType) {
        LOGGER.info(String.format("Getting existing %s in region %s", resourceType, region));

        String url = String.format("%s/%s/%s", getBaseUrl(region), category, resourceType);
        JsonNode jsonNode = null;
        try {
            jsonNode = getJsonNodeFromUrl(url);
        } catch (IOException e) {
            LOGGER.error(String.format("Failed to get Jason node from edda for existing %s.", resourceType), e);
        }

        Set<String> resourceIds = Sets.newHashSet();

        if (jsonNode == null || !jsonNode.isArray()) {
            throw new RuntimeException(String.format("Failed to get valid document from %s, got: %s", url, jsonNode));
        }

        for (Iterator<JsonNode> it = jsonNode.getElements(); it.hasNext();) {
            resourceIds.add(it.next().getTextValue());
        }

        LOGGER.info(String.format("Got %d existing %s in region %s.", resourceIds.size(), resourceType, region));
        return resourceIds;

    }

    @Override
    public String getBaseUrl(String region) {
        Validate.notEmpty(region);
        String baseUrl = config.getStr("edda.endpoint." + region);
        if (StringUtils.isBlank(baseUrl)) {
            LOGGER.error("No endpoint of Edda is found.");
        }
        return baseUrl;
    }
}
