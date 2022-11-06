package org.entur.bahamut.stopPlaces.stopPlacePopularityCache;

import java.util.Map;

public record StopPlacesPopularityCache(Map<String, Long> popularityPerStopPlaceId) {

    public Long getPopularity(String stopPlaceId) {
        return popularityPerStopPlaceId.get(stopPlaceId);
    }
}


