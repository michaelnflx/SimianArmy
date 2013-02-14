package com.netflix.simianarmy.aws.janitor.rule.volume.edda;

import com.netflix.simianarmy.Resource;
import com.netflix.simianarmy.janitor.Rule;

/**
 * The rule is for checking whether an EBS volume that had the deleteOnTermination flag set to true with its
 * last attachement and the attached instance is already terminated. Such volumes may exist due to AWS fails
 * to delete them in some edge cases (e.g. the requests to delete the volumes timed out).
 */
public class OrphanedVolumeRule implements Rule {

    @Override
    public boolean isValid(Resource resource) {
        return false;
    }
}
