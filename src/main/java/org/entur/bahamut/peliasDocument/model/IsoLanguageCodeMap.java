package org.entur.bahamut.peliasDocument.model;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

public class IsoLanguageCodeMap {
    private static final Map<String, Locale> isoLanguageCodeMap;

    public static String getLanguage(String iso3Code) {
        return isoLanguageCodeMap.get(iso3Code).getLanguage();
    }

    static {
        String[] languages = Locale.getISOLanguages();
        isoLanguageCodeMap = new HashMap<>(languages.length);
        for (String language : languages) {
            Locale locale = new Locale(language);
            isoLanguageCodeMap.put(locale.getISO3Language(), locale);
        }
    }
}
