package org.entur.bahamut.stopPlaces;

import org.entur.bahamut.Utilities;
import org.rutebanken.netex.model.BusSubmodeEnumeration;
import org.rutebanken.netex.model.StopPlace;
import org.rutebanken.netex.model.VehicleModeEnumeration;
import org.springframework.util.CollectionUtils;

public class StopPlaceValidator {

    private static final String KEY_IS_PARENT_STOP_PLACE = "IS_PARENT_STOP_PLACE";

    public static boolean isValid(StopPlace place) {
        // Ignore rail replacement bus
        if (VehicleModeEnumeration.BUS.equals(place.getTransportMode())
                && BusSubmodeEnumeration.RAIL_REPLACEMENT_BUS.equals(place.getBusSubmode())) {
            return false;
        }

        // Skip stops without quays, unless they are parent stops
        if (isQuayLessNonParentStop(place)) {
            return false;
        }

        return CollectionUtils.isEmpty(place.getValidBetween())
                || place.getValidBetween().stream().anyMatch(Utilities::isValidNow);
    }

    private static boolean isQuayLessNonParentStop(StopPlace place) {
        if (place.getQuays() == null || CollectionUtils.isEmpty(place.getQuays().getQuayRefOrQuay())) {
            return place.getKeyList() == null
                    || place.getKeyList().getKeyValue().stream()
                    .noneMatch(
                            kv -> KEY_IS_PARENT_STOP_PLACE.equals(kv.getKey())
                                    && Boolean.TRUE.toString().equalsIgnoreCase(kv.getValue()));
        }
        return false;
    }
}
