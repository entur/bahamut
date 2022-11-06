package org.entur.bahamut;

import org.rutebanken.netex.model.MultilingualString;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class Utilities {

    private static List<MultilingualString> filterUnique(List<MultilingualString> strings) {
        return strings.stream().filter(distinctByKey(MultilingualString::getValue)).collect(Collectors.toList());
    }

    private static <T> Predicate<T> distinctByKey(Function<? super T, ?> keyExtractor) {
        Map<Object, Boolean> seen = new ConcurrentHashMap<>();
        return t -> seen.putIfAbsent(keyExtractor.apply(t), Boolean.TRUE) == null;
    }

}
