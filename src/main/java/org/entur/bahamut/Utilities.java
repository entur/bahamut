package org.entur.bahamut;

import org.rutebanken.netex.model.MultilingualString;
import org.rutebanken.netex.model.ValidBetween;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.entur.geocoder.Utilities.requiredValidString;

public class Utilities {

    public static List<MultilingualString> filterUnique(List<MultilingualString> strings) {
        return strings.stream().filter(distinctByKey(MultilingualString::getValue)).collect(Collectors.toList());
    }

    private static <T> Predicate<T> distinctByKey(Function<? super T, ?> keyExtractor) {
        Map<Object, Boolean> seen = new ConcurrentHashMap<>();
        return t -> seen.putIfAbsent(keyExtractor.apply(t), Boolean.TRUE) == null;
    }

    /**
     * Should compare instant with valid between from/to in timezone defined in PublicationDelivery,
     * but makes little difference in practice
     **/
    public static boolean isValidNow(ValidBetween validBetween) {
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
