package org.entur.bahamut.groupOfStopPlaces;

import org.entur.bahamut.Utilities;
import org.entur.bahamut.data.BahamutData;
import org.entur.bahamut.stopPlaces.stopPlacePopularityCache.StopPlacesPopularityCache;
import org.entur.geocoder.model.PeliasDocument;
import org.entur.geocoder.model.PeliasId;
import org.rutebanken.netex.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.entur.bahamut.Utilities.filterUnique;

/**
 * Map NeTEx GroupOfStopPlaces objects to Pelias documents.
 */
@Component
public class GroupOfStopPlacesPeliasDocumentMapper {

    private static final Logger logger = LoggerFactory.getLogger(GroupOfStopPlacesPeliasDocumentMapper.class);

    public static final String GROUP_OF_STOP_PLACES_CATEGORY = "GroupOfStopPlaces";
    public static final String GROUP_OF_STOP_PLACE_LAYER = "GroupOfStopPlaces";

    private final GroupOfStopPlacesBoostConfiguration groupOfStopPlacesBoostConfiguration;

    public GroupOfStopPlacesPeliasDocumentMapper(GroupOfStopPlacesBoostConfiguration groupOfStopPlacesBoostConfiguration) {
        this.groupOfStopPlacesBoostConfiguration = groupOfStopPlacesBoostConfiguration;
    }

    public Stream<PeliasDocument> toPeliasDocuments(BahamutData bahamutData) {
        return bahamutData.groupOfStopPlaces().stream()
                .flatMap(groupOfStopPlaces ->
                        toPeliasDocumentsForNames(groupOfStopPlaces,
                                bahamutData.stopPlacesPopularityCache()))
                .filter(GroupOfStopPlacesPeliasDocumentMapper::isValidPeliasDocument);
    }

    public static boolean isValidPeliasDocument(PeliasDocument peliasDocument) {
        if (peliasDocument.getCenterPoint() == null) {
            logger.debug("Removing invalid document where geometry is missing:" + peliasDocument.getPeliasId());
            return false;
        }
        return true;
    }

    /**
     * Map single GroupOfStopPlaces to (potentially) multiple pelias documents, one per alias/alternative name.
     * <p>
     * Pelias does not yet support queries in multiple languages / for aliases. When support for this is ready this mapping should be
     * refactored to produce a single document per GoS.
     */
    public Stream<PeliasDocument> toPeliasDocumentsForNames(GroupOfStopPlaces groupOfStopPlaces,
                                                            StopPlacesPopularityCache stopPlacesPopularityCache) {

        if (!isValidGroupOfStopPlaces(groupOfStopPlaces)) {
            return Stream.empty();
        }

        AtomicInteger cnt = new AtomicInteger();

        return getNames(groupOfStopPlaces).stream()
                .map(documentName -> toPeliasDocument(
                        createId(groupOfStopPlaces, cnt),
                        documentName,
                        groupOfStopPlacesBoostConfiguration.getPopularity(groupOfStopPlaces, stopPlacesPopularityCache),
                        groupOfStopPlaces));
    }

    private String createId(GroupOfStopPlaces stopPlace, AtomicInteger documentIndex) {
        var idSuffix = documentIndex.getAndAdd(1) > 0 ? "-" + documentIndex.getAndAdd(1) : "";
        return stopPlace.getId() + idSuffix;
    }

    private static List<MultilingualString> getNames(GroupOfStopPlaces groupOfStopPlaces) {
        List<MultilingualString> names = new ArrayList<>();
        if (groupOfStopPlaces.getName() != null) {
            names.add(groupOfStopPlaces.getName());
        }

        if (groupOfStopPlaces.getAlternativeNames() != null
                && !CollectionUtils.isEmpty(groupOfStopPlaces.getAlternativeNames().getAlternativeName())) {
            groupOfStopPlaces.getAlternativeNames().getAlternativeName().stream()
                    .filter(an -> an.getName() != null && an.getName().getLang() != null)
                    .forEach(n -> names.add(n.getName()));
        }

        return filterUnique(names);
    }

    public static boolean isValidGroupOfStopPlaces(GroupOfStopPlaces object) {
        return CollectionUtils.isEmpty(object.getValidBetween())
                || object.getValidBetween().stream().anyMatch(Utilities::isValidNow);
    }

    private static PeliasDocument toPeliasDocument(String documentId,
                                                   MultilingualString documentName,
                                                   Long documentPopularity,
                                                   GroupOfStopPlaces groupOfStopPlaces) {

        PeliasId peliasId = PeliasId.of(documentId).withLayer(GROUP_OF_STOP_PLACE_LAYER);

        return new GroupOfStopPlacePeliasDocumentBuilder(peliasId)
                .withDocumentName(documentName)
                .withCategory(GROUP_OF_STOP_PLACES_CATEGORY)
                .withPopularity(documentPopularity)
                .withGroupOfStopPlaces(groupOfStopPlaces)
                .build();
    }
}
