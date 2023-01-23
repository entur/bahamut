package org.entur.bahamut.stopPlaces;

import org.entur.bahamut.stopPlaces.stopPlaceHierarchy.StopPlaceHierarchy;
import org.rutebanken.netex.model.AlternativeName;
import org.rutebanken.netex.model.MultilingualString;
import org.rutebanken.netex.model.NameTypeEnumeration;
import org.rutebanken.netex.model.StopPlace;
import org.springframework.util.CollectionUtils;

import java.util.Collections;
import java.util.List;

public class StopPlaceUtilities {

    public static List<AlternativeName> findAlternativeNameTranslations(StopPlace place) {
        return findAlternativeNameForNameType(place, NameTypeEnumeration.TRANSLATION);
    }

    public static List<AlternativeName> findAlternativeNameLabels(StopPlaceHierarchy placeHierarchy) {
        StopPlace place = placeHierarchy.place();
        List<AlternativeName> alternativeNames = findAlternativeNameForNameType(place, NameTypeEnumeration.LABEL);
        if (alternativeNames.isEmpty() && placeHierarchy.parent() != null) {
            return findAlternativeNameLabels(placeHierarchy.parent());
        }
        return alternativeNames;
    }

    private static List<AlternativeName> findAlternativeNameForNameType(StopPlace place, NameTypeEnumeration nameType) {
        if (place.getAlternativeNames() != null && !CollectionUtils.isEmpty(place.getAlternativeNames().getAlternativeName())) {
            return place.getAlternativeNames().getAlternativeName().stream()
                    .filter(alternativeName ->
                            nameType.equals(alternativeName.getNameType())
                                    && alternativeName.getName() != null && alternativeName.getName().getLang() != null)
                    .toList();
        }
        return Collections.emptyList();
    }

    /**
     * Get name from current place or, if not set, on closest parent with name set.
     * TODO: Use this to set the DefaultName ???
     *  It was used to set the DisplayName which is later copied to DefaultName in Entur-PeliasApi.
     */
    public static MultilingualString getClosestAvailableName(StopPlaceHierarchy placeHierarchy) {
        if (placeHierarchy.place().getName() != null) {
            return placeHierarchy.place().getName();
        }
        if (placeHierarchy.parent() != null) {
            return getClosestAvailableName(placeHierarchy.parent());
        }
        return null;
    }
}
