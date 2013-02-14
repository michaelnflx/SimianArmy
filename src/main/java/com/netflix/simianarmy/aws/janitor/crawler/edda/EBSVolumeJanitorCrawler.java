package com.netflix.simianarmy.aws.janitor.crawler.edda;

import com.amazonaws.services.ec2.model.Tag;
import com.amazonaws.services.ec2.model.Volume;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.simianarmy.Resource;
import com.netflix.simianarmy.aws.AWSResource;
import com.netflix.simianarmy.aws.AWSResourceType;
import com.netflix.simianarmy.aws.janitor.VolumeTaggingMonkey;
import com.netflix.simianarmy.client.edda.EddaClient;
import com.netflix.simianarmy.janitor.JanitorCrawler;
import com.netflix.simianarmy.janitor.JanitorMonkey;
import org.apache.commons.lang.Validate;
import org.codehaus.jackson.JsonNode;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Date;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * The crawler to crawl AWS EBS volumes for Janitor monkey using Edda.
 */
public class EBSVolumeJanitorCrawler implements JanitorCrawler {
    /** The Constant LOGGER. */
    private static final Logger LOGGER = LoggerFactory.getLogger(EBSVolumeJanitorCrawler.class);

    public static final DateTimeFormatter TIME_FORMATTER = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.S'Z'");

    private final EddaClient eddaClient;
    private final List<String> regions = Lists.newArrayList();

    /**
     * The constructor.
     * @param eddaClient the Edda client
     */
    public EBSVolumeJanitorCrawler(EddaClient eddaClient, String... regions) {
        Validate.notNull(eddaClient);
        this.eddaClient = eddaClient;
        Validate.notNull(regions);
        for (String region : regions) {
            this.regions.add(region);
        }
    }

    @Override
    public EnumSet<?> resourceTypes() {
        return EnumSet.of(AWSResourceType.EBS_VOLUME);
    }

    @Override
    public List<Resource> resources(Enum resourceType) {
        if ("EBS_VOLUME".equals(resourceType.name())) {
            return getVolumeResources();
        }
        return Collections.emptyList();
    }

    @Override
    public List<Resource> resources(String... resourceIds) {
        return getVolumeResources(resourceIds);
    }

    private List<Resource> getVolumeResources(String... volumeIds) {
        List<Resource> resources = new LinkedList<Resource>();

//        String urlVolumes = eddaClient.getBaseUrl()
//        JsonNode jsonNode = eddaClient.getJsonNodeFromUrl()
//
//        for (Volume volume : eddaClient.describeVolumes(volumeIds)) {
//            Resource volumeResource = new AWSResource().withId(volume.getVolumeId())
//                    .withRegion(getAWSClient().region()).withResourceType(AWSResourceType.EBS_VOLUME)
//                    .withLaunchTime(volume.getCreateTime());
//            for (Tag tag : volume.getTags()) {
//                LOGGER.info(String.format("Adding tag %s = %s to resource %s",
//                        tag.getKey(), tag.getValue(), volumeResource.getId()));
//                volumeResource.setTag(tag.getKey(), tag.getValue());
//            }
//            volumeResource.setOwnerEmail(getOwnerEmailForResource(volumeResource));
//            volumeResource.setDescription(getVolumeDescription(volume));
//            ((AWSResource) volumeResource).setAWSResourceState(volume.getState());
//            resources.add(volumeResource);
//        }
        return resources;
    }

    private Resource parseJsonElementToVolumeResource(String region, JsonNode jsonNode) {
        Validate.notNull(jsonNode);
        String createTimeText = jsonNode.get("createTime").getTextValue();

        Resource resource = new AWSResource().withId(jsonNode.get("volumeId").getTextValue()).withRegion(region)
                .withResourceType(AWSResourceType.EBS_VOLUME)
                .withLaunchTime(new Date(TIME_FORMATTER.parseDateTime(createTimeText).getMillis()));

        JsonNode attachments = jsonNode.get("attachments");
        if (attachments == null || !attachments.isArray() || attachments.size() == 0) {
            //TODO zhefu change to debug
            LOGGER.info(String.format("No attachments is found for %s", resource.getId()));
            //TODO zhefu find attachments from history
            //? any query can get all latest attachments from history?
        }
        return resource;
    }



    /**
     * Gets the volume attachments in the form of the map from volume to instance, with the
     * 'Delete on Termination' flag is or was set to true on its last attachment.
     * @return
     */
    private Map<String, String> getVolumeAttachmentsWithDeleteOnTermination(String region) {
        String url = eddaClient.getBaseUrl(region) + "/aws/volumes;attachments.deleteOnTermination=true;" +
                "_since=0;_expand:(volumeId,attachments:(instanceId,attachTime))";

        LOGGER.info(String.format("Getting volumes with deleteOnTermination flag set in region %s", region));
//        JsonNode jsonNode = null;
//        try {
////            jsonNode = getJsonNodeFromUrl(url);
//        } catch (IOException e) {
//            LOGGER.error("Failed to get Jason node from edda for volumes that had DeleteOnTermination set.", e);
//        }
//
//        Map<String, String> volumeToInstance = Maps.newHashMap();
//
//        if (jsonNode == null || !jsonNode.isArray()) {
//            throw new RuntimeException(String.format("Failed to get valid document from %s, got: %s", url, jsonNode));
//        }
//
//        for (Iterator<JsonNode> it = jsonNode.getElements(); it.hasNext();) {
//            JsonNode elem = it.next();
//            JsonNode volumeId = elem.get("volumeId");
//            JsonNode attachments = elem.get("attachments");
//            if (volumeId == null || attachments == null) {
//                throw new RuntimeException(String.format("Failed to find volumeId or attachments from: %s", jsonNode));
//            }
//            if (!attachments.isArray()) {
//                throw new RuntimeException(String.format("Failed to get valid attachments: %s", attachments));
//            }
//            Iterator<JsonNode> attachment = attachments.getElements();
//            JsonNode instanceId = null;
//            if (attachment.hasNext()) {
//                instanceId = attachment.next().get("instanceId");
//            }
//            if (instanceId == null) {
//                throw new RuntimeException(String.format("Failed to get valid attachments: %s", attachments));
//            }
//            volumeToInstance.put(volumeId.getTextValue(), instanceId.getTextValue());
//        }
//        LOGGER.info(String.format("Found %d volumes with deleteOnTermination' flag set in region %s",
//                volumeToInstance.size(), region));
//        return volumeToInstance;
        return null;
    }

    private String getVolumeDescription(Volume volume) {
        StringBuilder description = new StringBuilder();
        Integer size = volume.getSize();
        description.append(String.format("size=%s", size == null ? "unknown" : size));
        for (Tag tag : volume.getTags()) {
            description.append(String.format("; %s=%s", tag.getKey(), tag.getValue()));
        }
        return description.toString();
    }

    @Override
    public String getOwnerEmailForResource(Resource resource) {
//        String owner = super.getOwnerEmailForResource(resource);
//        if (owner == null) {
//            // try to find the owner from Janitor Metadata tag set by the volume tagging monkey.
//            Map<String, String> janitorTag = VolumeTaggingMonkey.parseJanitorMetaTag(resource.getTag(
//                    JanitorMonkey.JANITOR_META_TAG));
//            owner = janitorTag.get(JanitorMonkey.OWNER_TAG_KEY);
//        }
//        return owner;
        return null;
    }


}
