package org.entur.bahamut.peliasDocument.stopPlacestoPeliasDocument;

import org.rutebanken.netex.model.BusSubmodeEnumeration;
import org.rutebanken.netex.model.StopPlace;
import org.rutebanken.netex.model.ValidBetween;
import org.rutebanken.netex.model.VehicleModeEnumeration;
import org.springframework.util.CollectionUtils;

import java.time.LocalDateTime;

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
                || place.getValidBetween().stream().anyMatch(StopPlaceValidator::isValidNow);
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

    // Should compare instant with valid between from/to in timezone defined in PublicationDelivery,
    // but makes little difference in practice
    private static boolean isValidNow(ValidBetween validBetween) {
        var now = LocalDateTime.now();
        if (validBetween != null) {
            if (validBetween.getFromDate() != null && validBetween.getFromDate().isAfter(now)) {
                return false;
            }

            return validBetween.getToDate() == null || !validBetween.getToDate().isBefore(now);
        }
        return true;
    }
}
