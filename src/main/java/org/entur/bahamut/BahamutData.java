package org.entur.bahamut;

import org.entur.bahamut.stopPlaces.stopPlaceHierarchy.StopPlaceHierarchies;
import org.entur.bahamut.stopPlaces.stopPlaceHierarchy.StopPlaceHierarchy;
import org.entur.netex.index.api.NetexEntitiesIndex;
import org.rutebanken.netex.model.GroupOfStopPlaces;

import java.util.Collection;
import java.util.List;

public record BahamutData(
        List<StopPlaceHierarchy> stopPlaceHierarchies,
        List<GroupOfStopPlaces> groupOfStopPlaces) {

    public static BahamutData create(NetexEntitiesIndex netexEntitiesIndex) {
        List<StopPlaceHierarchy> stopPlaceHierarchies = netexEntitiesIndex.getSiteFrames().stream()
                .map(siteFrame -> siteFrame.getStopPlaces().getStopPlace())
                .map(StopPlaceHierarchies::create)
                .flatMap(Collection::stream)
                .toList();

        List<GroupOfStopPlaces> groupOfStopPlaces = netexEntitiesIndex.getSiteFrames().stream()
                .map(siteFrame -> siteFrame.getGroupsOfStopPlaces().getGroupOfStopPlaces())
                .flatMap(Collection::stream)
                .toList();

        return new BahamutData(stopPlaceHierarchies, groupOfStopPlaces);
    }
}
