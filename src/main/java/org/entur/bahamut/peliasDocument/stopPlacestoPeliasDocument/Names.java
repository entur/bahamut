package org.entur.bahamut.peliasDocument.stopPlacestoPeliasDocument;

import org.entur.bahamut.peliasDocument.model.PeliasDocument;
import org.entur.bahamut.peliasDocument.stopPlaceHierarchy.StopPlaceHierarchy;
import org.rutebanken.netex.model.MultilingualString;
import org.rutebanken.netex.model.NameTypeEnumeration;
import org.rutebanken.netex.model.StopPlace;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class Names {

    static void setDefaultName(PeliasDocument document, MultilingualString name) {
        if (name != null) {
            document.addDefaultName(name.getValue());
        }
    }

    /**
     * Add official name as display name. Not a part of standard pelias model,
     * will be copied to name.default before deduping and labelling in Entur-pelias API.
     */
    static void setDisplayName(PeliasDocument document, StopPlaceHierarchy stopPlaceHierarchy) {
        var displayName = getClosestAvailableName(stopPlaceHierarchy);
        if (displayName != null) {
            document.addDisplayName(displayName.getValue());
            if (displayName.getLang() != null) {
                document.addName(displayName.getLang(), displayName.getValue());
            }
        }
    }

    static void setAlternativeNames(PeliasDocument document, StopPlace place) {
        if (place.getAlternativeNames() != null
                && !CollectionUtils.isEmpty(place.getAlternativeNames().getAlternativeName())) {
            place.getAlternativeNames().getAlternativeName().stream()
                    .filter(alternativeName ->
                            NameTypeEnumeration.TRANSLATION.equals(alternativeName.getNameType())
                                    && alternativeName.getName() != null && alternativeName.getName().getLang() != null)
                    .forEach(n -> document.addName(n.getName().getLang(), n.getName().getValue()));
        }
    }

    /**
     * Add alternative names with type 'label' to alias map. Use parents values if not set on stop place.
     */
    static void setAlternativeNameLabels(PeliasDocument document, StopPlaceHierarchy placeHierarchy) {
        StopPlace place = placeHierarchy.getPlace();
        if (place.getAlternativeNames() != null && !CollectionUtils.isEmpty(place.getAlternativeNames().getAlternativeName())) {
            place.getAlternativeNames().getAlternativeName().stream()
                    .filter(alternativeName ->
                            NameTypeEnumeration.LABEL.equals(alternativeName.getNameType())
                                    && alternativeName.getName() != null
                    ).forEach(alternativeName -> {
                        if (alternativeName.getName().getLang() != null) {
                            document.addAlias(alternativeName.getName().getLang(), alternativeName.getName().getValue());
                        } else {
                            document.addDefaultAlias(alternativeName.getName().getValue());
                        }
                    });
        }
        if ((document.aliasMap() == null || document.aliasMap().isEmpty()) && placeHierarchy.getParent() != null) {
            setAlternativeNameLabels(document, placeHierarchy.getParent());
        }
    }

    static List<MultilingualString> getNames(StopPlaceHierarchy placeHierarchy) {
        List<MultilingualString> names = new ArrayList<>();

        collectNames(placeHierarchy, names, true);
        collectNames(placeHierarchy, names, false);

        return filterUnique(names);
    }

    /**
     * Get name from current place or, if not set, on closest parent with name set.
     */
    private static MultilingualString getClosestAvailableName(StopPlaceHierarchy placeHierarchy) {
        if (placeHierarchy.getPlace().getName() != null) {
            return placeHierarchy.getPlace().getName();
        }
        if (placeHierarchy.getParent() != null) {
            return getClosestAvailableName(placeHierarchy.getParent());
        }
        return null;
    }

    private static void collectNames(StopPlaceHierarchy placeHierarchy, List<MultilingualString> names, boolean up) {
        StopPlace place = placeHierarchy.getPlace();
        if (place.getName() != null) {
            names.add(place.getName());
        }

        if (place.getAlternativeNames() != null
                && !CollectionUtils.isEmpty(place.getAlternativeNames().getAlternativeName())) {

            place.getAlternativeNames().getAlternativeName().stream()
                    .filter(alternativeName ->
                            alternativeName.getName() != null
                                    && (NameTypeEnumeration.LABEL.equals(alternativeName.getNameType())
                                    || alternativeName.getName().getLang() != null)
                    ).forEach(n -> names.add(n.getName()));
        }

        if (up) {
            if (placeHierarchy.getParent() != null) {
                collectNames(placeHierarchy.getParent(), names, up);
            }
        } else {
            if (!CollectionUtils.isEmpty(placeHierarchy.getChildren())) {
                placeHierarchy.getChildren().forEach(child -> collectNames(child, names, up));
            }
        }
    }

    private static List<MultilingualString> filterUnique(List<MultilingualString> strings) {
        return strings.stream().filter(distinctByKey(MultilingualString::getValue)).collect(Collectors.toList());
    }

    private static <T> Predicate<T> distinctByKey(Function<? super T, ?> keyExtractor) {
        Map<Object, Boolean> seen = new ConcurrentHashMap<>();
        return t -> seen.putIfAbsent(keyExtractor.apply(t), Boolean.TRUE) == null;
    }
}
