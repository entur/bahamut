package org.entur.bahamut.data;

import org.entur.bahamut.stopPlaces.stopPlaceHierarchy.StopPlaceHierarchies;
import org.entur.bahamut.stopPlaces.stopPlaceHierarchy.StopPlaceHierarchy;
import org.entur.bahamut.stopPlaces.stopPlacePopularityCache.StopPlacesPopularityCache;
import org.entur.bahamut.stopPlaces.stopPlacePopularityCache.StopPlacesPopularityCacheBuilder;
import org.entur.netex.index.api.NetexEntitiesIndex;
import org.rutebanken.netex.model.GroupOfStopPlaces;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.List;

@Component
public class BahamutDataBuilder {

    private static final Logger logger = LoggerFactory.getLogger(BahamutData.class);

    private final StopPlacesPopularityCacheBuilder stopPlacesPopularityCacheBuilder;

    public BahamutDataBuilder(StopPlacesPopularityCacheBuilder stopPlacesPopularityCacheBuilder) {
        this.stopPlacesPopularityCacheBuilder = stopPlacesPopularityCacheBuilder;
    }

    public BahamutData build(NetexEntitiesIndex netexEntitiesIndex) {

        logger.info("Creating the stop place hierarchies");
        List<StopPlaceHierarchy> stopPlaceHierarchies = netexEntitiesIndex.getSiteFrames().stream()
                .map(siteFrame -> siteFrame.getStopPlaces().getStopPlace())
                .map(StopPlaceHierarchies::create)
                .flatMap(Collection::stream)
                .toList();

        logger.info("Getting group of stop places");
        List<GroupOfStopPlaces> groupOfStopPlaces = netexEntitiesIndex.getSiteFrames().stream()
                .map(siteFrame -> siteFrame.getGroupsOfStopPlaces().getGroupOfStopPlaces())
                .flatMap(Collection::stream)
                .toList();

        logger.info("Calculating and caching stop places popularity");
        StopPlacesPopularityCache stopPlacesPopularityCache =
                stopPlacesPopularityCacheBuilder.build(stopPlaceHierarchies);

        return new BahamutData(stopPlaceHierarchies, groupOfStopPlaces, stopPlacesPopularityCache);
    }
}
