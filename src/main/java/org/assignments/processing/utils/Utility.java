package org.assignments.processing.utils;

import org.apache.commons.lang3.StringUtils;

import java.util.UUID;

public class Utility {
    public static UUID getUUid1(String uuid){
        return UUID.fromString(StringUtils.substring(uuid, 4));
    }
}
