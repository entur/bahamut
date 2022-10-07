package org.entur.bahamut.stopPlaces.stopPlaceHierarchy;

import org.rutebanken.netex.model.StopPlace;

import java.util.*;
import java.util.stream.Collectors;

public class StopPlaceHierarchies {

    public static Set<StopPlaceHierarchy> create(List<StopPlace> places) {
        var childStopPlacesByParentRef = places.stream()
                .filter(sp -> sp.getParentSiteRef() != null)
                .collect(Collectors.groupingBy(sp -> sp.getParentSiteRef().getRef()));

        var stopPlaceHierarchies = places.stream()
                .filter(sp -> sp.getParentSiteRef() == null)
                .map(sp -> createHierarchyForStopPlace(sp, null, childStopPlacesByParentRef))
                .toList();

        var allStopPlaces = new HashSet<StopPlaceHierarchy>();
        expandStopPlaceHierarchies(stopPlaceHierarchies, allStopPlaces);
        return allStopPlaces;
    }

    private static void expandStopPlaceHierarchies(Collection<StopPlaceHierarchy> hierarchies,
                                                   Set<StopPlaceHierarchy> target) {
        if (hierarchies != null) {
            for (var stopPlacePlaceHierarchy : hierarchies) {
                target.add(stopPlacePlaceHierarchy);
                expandStopPlaceHierarchies(stopPlacePlaceHierarchy.children(), target);
            }
        }
    }

    private static StopPlaceHierarchy createHierarchyForStopPlace(StopPlace stopPlace,
                                                                  StopPlaceHierarchy parent,
                                                                  Map<String, List<StopPlace>> childrenByParentIdMap) {
        var children = childrenByParentIdMap.get(stopPlace.getId());
        List<StopPlaceHierarchy> childHierarchies = new ArrayList<>();
        StopPlaceHierarchy hierarchy = new StopPlaceHierarchy(stopPlace, parent);
        if (children != null) {
            childHierarchies = children.stream()
                    .map(child -> createHierarchyForStopPlace(child, hierarchy, childrenByParentIdMap))
                    .toList();
        }
        hierarchy.setChildren(childHierarchies);
        return hierarchy;
    }
}
