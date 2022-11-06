package org.entur.bahamut.stopPlaces.stopPlacePopularityCache;

import org.entur.bahamut.stopPlaces.boostConfiguration.StopPlaceBoostConfiguration;
import org.entur.bahamut.stopPlaces.stopPlaceHierarchy.StopPlaceHierarchy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Component
public class StopPlacesPopularityCacheBuilder {

    private final StopPlaceBoostConfiguration stopPlaceBoostConfiguration;

    @Autowired
    public StopPlacesPopularityCacheBuilder(StopPlaceBoostConfiguration stopPlaceBoostConfiguration) {
        this.stopPlaceBoostConfiguration = stopPlaceBoostConfiguration;
    }

    public StopPlacesPopularityCache build(List<StopPlaceHierarchy> stopPlaceHierarchies) {
        Map<String, Long> collect = stopPlaceHierarchies.stream()
                .collect(Collectors.toMap(
                        stopPlaceHierarchy -> stopPlaceHierarchy.place().getId(),
                        stopPlaceBoostConfiguration::getPopularity
                ));

        return new StopPlacesPopularityCache(collect);
    }
}
