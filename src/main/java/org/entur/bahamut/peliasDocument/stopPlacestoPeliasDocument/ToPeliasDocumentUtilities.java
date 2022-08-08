package org.entur.bahamut.peliasDocument.stopPlacestoPeliasDocument;

import org.entur.bahamut.peliasDocument.model.AddressParts;
import org.entur.bahamut.peliasDocument.model.PeliasDocument;
import org.rutebanken.netex.model.GroupOfEntities_VersionStructure;
import org.rutebanken.netex.model.MultilingualString;
import org.rutebanken.netex.model.ValidBetween;
import org.springframework.util.CollectionUtils;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class ToPeliasDocumentUtilities {

    protected static final String DEFAULT_LANGUAGE = "nor";

    public static List<MultilingualString> filterUnique(List<MultilingualString> strings) {
        return strings.stream().filter(distinctByKey(MultilingualString::getValue)).collect(Collectors.toList());
    }

    public static boolean isValid(GroupOfEntities_VersionStructure object) {
        return CollectionUtils.isEmpty(object.getValidBetween())
                || object.getValidBetween().stream().anyMatch(ToPeliasDocumentUtilities::isValidNow);
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

    protected static <T> Predicate<T> distinctByKey(Function<? super T, ?> keyExtractor) {
        Map<Object, Boolean> seen = new ConcurrentHashMap<>();
        return t -> seen.putIfAbsent(keyExtractor.apply(t), Boolean.TRUE) == null;
    }

    /**
     * The Pelias APIs de-duper will throw away results with identical name, layer, parent and address.
     * Setting unique ID in street part of address to avoid unique topographic places with identical
     * names being de-duped.
     * TODO: DO we need this ???
     */
    public static void addIdToStreetNameToAvoidFalseDuplicates(String placeId, PeliasDocument document) {
        if (document.addressParts() == null) {
            document.setAddressParts(new AddressParts());
        }
        document.addressParts().setStreet("NOT_AN_ADDRESS-" + placeId);
    }
}
