package org.entur.bahamut.stopPlaces;

import org.entur.bahamut.data.BahamutData;
import org.entur.bahamut.stopPlaces.stopPlaceHierarchy.StopPlaceHierarchy;
import org.entur.bahamut.stopPlaces.stopPlacePopularityCache.StopPlacesPopularityCache;
import org.entur.geocoder.model.*;
import org.rutebanken.netex.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.entur.bahamut.Utilities.filterUnique;
import static org.entur.bahamut.stopPlaces.StopPlaceValidator.isValid;

@Component
public class StopPlacePeliasDocumentMapper {

    private static final Logger logger = LoggerFactory.getLogger(StopPlacePeliasDocumentMapper.class);

    public static final String STOP_PLACE_LAYER = "StopPlace";
    public static final String PARENT_STOP_PLACE_LAYER = "StopPlaceParent";
    public static final String CHILD_STOP_PLACE_LAYER = "StopPlaceChild";

    public Stream<PeliasDocument> toPeliasDocuments(BahamutData bahamutData) {
        return bahamutData.stopPlaceHierarchies().stream()
                .flatMap(stopPlaceHierarchy ->
                        toPeliasDocumentsForNames(stopPlaceHierarchy,
                                bahamutData.stopPlacesPopularityCache()))
                .filter(StopPlacePeliasDocumentMapper::isValidPeliasDocument);
    }

    public static boolean isValidPeliasDocument(PeliasDocument peliasDocument) {
        if (peliasDocument.getCenterPoint() == null) {
            logger.debug("Removing invalid document where geometry is missing:" + peliasDocument.getPeliasId());
            return false;
        }
        return true;
    }

    /**
     * TODO: Kan dette gjøres nå ???
     * Map single place hierarchy to (potentially) multiple pelias documents, one per alias/alternative name.
     * Pelias does not yet support queries in multiple languages / for aliases.
     * When support for this is ready this mapping should be refactored to produce
     * a single document per place hierarchy.
     */
    public Stream<PeliasDocument> toPeliasDocumentsForNames(StopPlaceHierarchy placeHierarchy,
                                                            StopPlacesPopularityCache stopPlacesPopularityCache) {
        var place = placeHierarchy.place();
        if (!isValid(place)) {
            return Stream.empty();
        }
        var cnt = new AtomicInteger();

        return getNames(placeHierarchy).stream()
                .map(documentName -> createPeliasDocument(
                        createId(place, cnt),
                        documentName,
                        stopPlacesPopularityCache.getPopularity(place.getId()),
                        placeHierarchy)
                );
    }

    private String createId(StopPlace stopPlace, AtomicInteger documentIndex) {
        var idSuffix = documentIndex.getAndAdd(1) > 0 ? "-" + documentIndex.getAndAdd(1) : "";
        return stopPlace.getId() + idSuffix;
    }

    private static PeliasDocument createPeliasDocument(String documentId,
                                                       MultilingualString documentName,
                                                       Long documentPopularity,
                                                       StopPlaceHierarchy placeHierarchy) {

        PeliasId peliasId = PeliasId.of(documentId).withLayer(getLayer(placeHierarchy));

        return new StopPlacePeliasDocumentBuilder(peliasId)
                .withDocumentName(documentName)
                .withPopularity(documentPopularity)
                .withStopPlaceHierarchy(placeHierarchy)
                .build();
    }

    private static List<MultilingualString> getNames(StopPlaceHierarchy placeHierarchy) {
        List<MultilingualString> names = new ArrayList<>();

        collectNames(placeHierarchy, names, true);
        collectNames(placeHierarchy, names, false);

        return filterUnique(names);
    }

    private static void collectNames(StopPlaceHierarchy placeHierarchy, List<MultilingualString> names, boolean up) {
        StopPlace place = placeHierarchy.place();
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
            if (placeHierarchy.parent() != null) {
                collectNames(placeHierarchy.parent(), names, up);
            }
        } else {
            if (!CollectionUtils.isEmpty(placeHierarchy.children())) {
                placeHierarchy.children().forEach(child -> collectNames(child, names, up));
            }
        }
    }

    /**
     * Categorize multimodal stops with separate layers in order to be able to filter in queries.
     * <p>
     * Multimodal parents with one layer
     * Multimodal children with another layer
     * Non-multimodal stops with default layer
     */
    private static String getLayer(StopPlaceHierarchy hierarchy) {
        if (hierarchy.parent() != null) {
            return CHILD_STOP_PLACE_LAYER;
        } else if (!CollectionUtils.isEmpty(hierarchy.children())) {
            return PARENT_STOP_PLACE_LAYER;
        }
        return STOP_PLACE_LAYER;
    }
}
