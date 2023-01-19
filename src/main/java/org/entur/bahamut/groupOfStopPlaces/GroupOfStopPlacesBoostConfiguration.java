package org.entur.bahamut.groupOfStopPlaces;

import org.entur.bahamut.stopPlaces.stopPlacePopularityCache.StopPlacesPopularityCache;
import org.rutebanken.netex.model.GroupOfStopPlaces;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Objects;

@Component
public class GroupOfStopPlacesBoostConfiguration {

    private final double gosBoostFactor;

    @Autowired
    public GroupOfStopPlacesBoostConfiguration(@Value("${pelias.gos.boost.factor:1.0}") double gosBoostFactor) {
        this.gosBoostFactor = gosBoostFactor;
    }

    public Long getPopularity(GroupOfStopPlaces groupOfStopPlaces, StopPlacesPopularityCache stopPlacesPopularityCache) {
        if (groupOfStopPlaces.getMembers() == null) {
            return null;
        }
        try {
            double popularity = gosBoostFactor * groupOfStopPlaces.getMembers().getStopPlaceRef().stream()
                    .map(sp -> stopPlacesPopularityCache.getPopularity(sp.getRef()))
                    .filter(Objects::nonNull)
                    .reduce(1L, Math::multiplyExact);

            return (long) popularity;
        } catch (ArithmeticException ae) {
            return Long.MAX_VALUE;
        }
    }
}
