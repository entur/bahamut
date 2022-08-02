package org.entur.bahamut.peliasDocument.placeHierarchy;

import org.rutebanken.netex.model.StopPlace;

import java.util.*;
import java.util.stream.Collectors;

public class PlaceHierarchies {

    public static Set<PlaceHierarchy<StopPlace>> create(List<StopPlace> places) {
        var childStopPlacesByParentRef = places.stream()
                .filter(sp -> sp.getParentSiteRef() != null)
                .collect(Collectors.groupingBy(sp -> sp.getParentSiteRef().getRef()));

        var stopPlaceHierarchies = places.stream()
                .filter(sp -> sp.getParentSiteRef() == null)
                .map(sp -> createHierarchyForStopPlace(sp, null, childStopPlacesByParentRef))
                .toList();

        var allStopPlaces = new HashSet<PlaceHierarchy<StopPlace>>();
        expandStopPlaceHierarchies(stopPlaceHierarchies, allStopPlaces);
        return allStopPlaces;
    }

    private static void expandStopPlaceHierarchies(Collection<PlaceHierarchy<StopPlace>> hierarchies,
                                                   Set<PlaceHierarchy<StopPlace>> target) {
        if (hierarchies != null) {
            for (var stopPlacePlaceHierarchy : hierarchies) {
                target.add(stopPlacePlaceHierarchy);
                expandStopPlaceHierarchies(stopPlacePlaceHierarchy.getChildren(), target);
            }
        }
    }

    private static PlaceHierarchy<StopPlace> createHierarchyForStopPlace(StopPlace stopPlace,
                                                                         PlaceHierarchy<StopPlace> parent,
                                                                         Map<String, List<StopPlace>> childrenByParentIdMap) {
        var children = childrenByParentIdMap.get(stopPlace.getId());
        List<PlaceHierarchy<StopPlace>> childHierarchies = new ArrayList<>();
        PlaceHierarchy<StopPlace> hierarchy = new PlaceHierarchy<>(stopPlace, parent);
        if (children != null) {
            childHierarchies = children.stream()
                    .map(child -> createHierarchyForStopPlace(child, hierarchy, childrenByParentIdMap))
                    .toList();
        }
        hierarchy.setChildren(childHierarchies);
        return hierarchy;
    }
}
