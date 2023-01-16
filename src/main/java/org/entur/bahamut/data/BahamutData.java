package org.entur.bahamut.data;

import org.entur.bahamut.stopPlaces.stopPlaceHierarchy.StopPlaceHierarchy;
import org.entur.bahamut.stopPlaces.stopPlacePopularityCache.StopPlacesPopularityCache;
import org.rutebanken.netex.model.GroupOfStopPlaces;

import java.util.List;

public record BahamutData(
        List<StopPlaceHierarchy> stopPlaceHierarchies,
        List<GroupOfStopPlaces> groupOfStopPlaces,
        StopPlacesPopularityCache stopPlacesPopularityCache) {
}
