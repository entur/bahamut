package org.entur.bahamut.groupOfStopPlaces;

import org.apache.commons.lang3.StringUtils;
import org.entur.bahamut.stopPlaces.stopPlacePopularityCache.StopPlacesPopularityCache;
import org.entur.geocoder.model.AddressParts;
import org.entur.geocoder.model.GeoPoint;
import org.entur.geocoder.model.PeliasDocument;
import org.rutebanken.netex.model.*;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Map NeTEx GroupOfStopPlaces objects to Pelias documents.
 */
@Component
public class GroupOfStopPlacesPeliasDocumentMapper {
    // Using substitute layer for GoS to avoid having to fork pelias (custom layers not configurable).
    public static final String GROUP_OF_STOP_PLACE_LAYER = "group_of_stop_places";
    public static final String DEFAULT_SOURCE = "nsr";
    private static final String DEFAULT_LANGUAGE = "nor";


    private final GroupOfStopPlacesBoostConfiguration groupOfStopPlacesBoostConfiguration;

    public GroupOfStopPlacesPeliasDocumentMapper(GroupOfStopPlacesBoostConfiguration groupOfStopPlacesBoostConfiguration) {
        this.groupOfStopPlacesBoostConfiguration = groupOfStopPlacesBoostConfiguration;
    }

    public Stream<PeliasDocument> toPeliasDocuments(List<GroupOfStopPlaces> listOfGroupOfStopPlaces,
                                                    StopPlacesPopularityCache stopPlacesPopularityCache) {
        return listOfGroupOfStopPlaces.stream()
                .flatMap(groupOfStopPlaces -> toPeliasDocumentsForNames(groupOfStopPlaces, stopPlacesPopularityCache))
                .sorted((p1, p2) -> -p1.getPopularity().compareTo(p2.getPopularity())) // TODO: Remove
                .filter(PeliasDocument::isValid);
    }

    /**
     * Map single GroupOfStopPlaces to (potentially) multiple pelias documents, one per alias/alternative name.
     * <p>
     * Pelias does not yet support queries in multiple languages / for aliases. When support for this is ready this mapping should be
     * refactored to produce a single document per GoS.
     */
    public Stream<PeliasDocument> toPeliasDocumentsForNames(GroupOfStopPlaces groupOfStopPlaces,
                                                            StopPlacesPopularityCache stopPlacesPopularityCache) {

        if (!isValid(groupOfStopPlaces)) {
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

    private String createId(GroupOfStopPlaces groupOfStopPlaces, AtomicInteger documentIndex) {
        var idSuffix = documentIndex.getAndAdd(1) > 0 ? "-" + documentIndex.getAndAdd(1) : "";
        return groupOfStopPlaces.getId() + idSuffix;
    }

    public static boolean isValid(GroupOfEntities_VersionStructure object) {
        return CollectionUtils.isEmpty(object.getValidBetween())
                || object.getValidBetween().stream().anyMatch(GroupOfStopPlacesPeliasDocumentMapper::isValidNow);
    }

    /*
     * Should compare instant with valid between from/to in timezone defined in PublicationDelivery,
     * but makes little difference in practice.
     */
    private static boolean isValidNow(ValidBetween validBetween) {
        LocalDateTime now = LocalDateTime.now();
        if (validBetween != null) {
            if (validBetween.getFromDate() != null && validBetween.getFromDate().isAfter(now)) {
                return false;
            }

            return validBetween.getToDate() == null || !validBetween.getToDate().isBefore(now);
        }
        return true;
    }

    private PeliasDocument toPeliasDocument(String documentId,
                                            MultilingualString documentName,
                                            Long documentPopularity,
                                            GroupOfStopPlaces groupOfStopPlaces) {

        PeliasDocument document = new PeliasDocument(GROUP_OF_STOP_PLACE_LAYER, DEFAULT_SOURCE, documentId);
        if (documentName != null) {
            document.setDefaultName(documentName.getValue());
        }

        /*
         * Add official name as display name. Not a part of standard pelias model,
         * will be copied to name.default before deduping and labelling in Entur-pelias API.
         */
        MultilingualString displayName = groupOfStopPlaces.getName();
        if (displayName != null) {
            document.setDisplayName(displayName.getValue());
            if (displayName.getLang() != null) {
                document.addAlternativeName(displayName.getLang(), displayName.getValue());
            }
        }

        if (groupOfStopPlaces.getCentroid() != null) {
            LocationStructure loc = groupOfStopPlaces.getCentroid().getLocation();
            document.setCenterPoint(new GeoPoint(loc.getLatitude().doubleValue(), loc.getLongitude().doubleValue()));
        }

        addIdToStreetNameToAvoidFalseDuplicates(groupOfStopPlaces, document);

        if (groupOfStopPlaces.getDescription() != null
                && !StringUtils.isEmpty(groupOfStopPlaces.getDescription().getValue())) {
            String lang = groupOfStopPlaces.getDescription().getLang();
            if (lang == null) {
                lang = DEFAULT_LANGUAGE;
            }
            document.addDescription(lang, groupOfStopPlaces.getDescription().getValue());
        }

        document.setPopularity(documentPopularity);
        document.addCategory(GroupOfStopPlaces.class.getSimpleName());

        return document;
    }

    /**
     * The Pelias APIs deduper will throw away results with identical name, layer, parent and address. Setting unique ID in street part of address to avoid unique
     * topographic places with identical names being deduped.
     */
    private void addIdToStreetNameToAvoidFalseDuplicates(GroupOfStopPlaces groupOfStopPlaces, PeliasDocument document) {
        document.setAddressParts(new AddressParts("NOT_AN_ADDRESS-" + groupOfStopPlaces.getId()));
    }

    private List<MultilingualString> getNames(GroupOfStopPlaces groupOfStopPlaces) {
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

    public static List<MultilingualString> filterUnique(List<MultilingualString> strings) {
        return strings.stream().filter(distinctByKey(MultilingualString::getValue)).collect(Collectors.toList());
    }

    protected static <T> Predicate<T> distinctByKey(Function<? super T, ?> keyExtractor) {
        Map<Object, Boolean> seen = new ConcurrentHashMap<>();
        return t -> seen.putIfAbsent(keyExtractor.apply(t), Boolean.TRUE) == null;
    }
}
