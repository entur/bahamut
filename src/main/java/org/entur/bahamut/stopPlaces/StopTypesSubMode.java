package org.entur.bahamut.stopPlaces;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.entur.bahamut.stopPlaces.stopPlaceHierarchy.StopPlaceHierarchy;
import org.rutebanken.netex.model.StopPlace;
import org.rutebanken.netex.model.StopTypeEnumeration;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class StopTypesSubMode {

    public static List<Pair<StopTypeEnumeration, Enum>> getStopTypeAndSubMode(StopPlaceHierarchy placeHierarchy) {
        List<Pair<StopTypeEnumeration, Enum>> types = new ArrayList<>();

        StopPlace stopPlace = placeHierarchy.place();

        types.add(new ImmutablePair<>(stopPlace.getStopPlaceType(), getStopSubMode(stopPlace)));

        if (!CollectionUtils.isEmpty(placeHierarchy.children())) {
            types.addAll(placeHierarchy.children().stream()
                    .map(StopTypesSubMode::getStopTypeAndSubMode)
                    .flatMap(Collection::stream).toList());
        }

        return types;
    }

    private static Enum getStopSubMode(StopPlace stopPlace) {

        if (stopPlace.getStopPlaceType() != null) {
            switch (stopPlace.getStopPlaceType()) {
                case AIRPORT -> stopPlace.getAirSubmode();
                case HARBOUR_PORT, FERRY_STOP, FERRY_PORT -> stopPlace.getWaterSubmode();
                case BUS_STATION, COACH_STATION, ONSTREET_BUS -> stopPlace.getBusSubmode();
                case RAIL_STATION -> stopPlace.getRailSubmode();
                case METRO_STATION -> stopPlace.getMetroSubmode();
                case ONSTREET_TRAM, TRAM_STATION -> stopPlace.getTramSubmode();
            }
        }
        return null;
    }
}
