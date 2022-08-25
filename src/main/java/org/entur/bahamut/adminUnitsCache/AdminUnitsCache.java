package org.entur.bahamut.adminUnitsCache;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.entur.netex.index.api.NetexEntitiesIndex;
import org.locationtech.jts.geom.Point;
import org.rutebanken.netex.model.GroupOfStopPlaces;
import org.rutebanken.netex.model.IanaCountryTldEnumeration;
import org.rutebanken.netex.model.TopographicPlace;
import org.rutebanken.netex.model.ValidBetween;

import java.time.LocalDateTime;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public record AdminUnitsCache(Cache<String, String> adminUnitNamesCache,
                              Map<String, AdminUnit> countries,
                              Map<String, AdminUnit> counties,
                              Map<String, AdminUnit> localities,
                              List<GroupOfStopPlaces> groupOfStopPlaces) {

    public static AdminUnitsCache buildNewCache(NetexEntitiesIndex netexEntitiesIndex, Integer cacheMaxSize) {

        var groupOfStopPlaces = netexEntitiesIndex.getSiteFrames().stream()
                .flatMap(siteFrame -> siteFrame.getGroupsOfStopPlaces().getGroupOfStopPlaces().stream())
                .toList();

        var allAdminUnits = netexEntitiesIndex.getSiteFrames().stream()
                .flatMap(siteFrame -> siteFrame.getTopographicPlaces().getTopographicPlace().stream())
                .filter(AdminUnitsCache::isCurrent)
                .filter(topographicPlace -> {
                    LocalDateTime toDate = topographicPlace.getValidBetween().get(0).getToDate();
                    return toDate == null || toDate.isAfter(LocalDateTime.now());
                })
                .filter(topographicPlace -> topographicPlace.getPolygon() != null)
                .map(AdminUnit::makeAdminUnit)
                .toList();

        var localities = allAdminUnits.stream()
                .filter(adminUnit -> adminUnit.adminUnitType() == AdminUnitType.LOCALITY)
                .collect(Collectors.toMap(AdminUnit::id, Function.identity()));

        var counties = allAdminUnits.stream()
                .filter(adminUnit -> adminUnit.adminUnitType() == AdminUnitType.COUNTY)
                .collect(Collectors.toMap(AdminUnit::id, Function.identity()));

        var countries = allAdminUnits.stream()
                .filter(adminUnit -> adminUnit.adminUnitType() == AdminUnitType.COUNTRY)
                .filter(adminUnit -> !adminUnit.countryRef().equals(IanaCountryTldEnumeration.RU.name()))
                .collect(Collectors.toMap(AdminUnit::id, Function.identity()));

        Cache<String, String> adminUnitNamesCache = CacheBuilder.newBuilder().maximumSize(cacheMaxSize).build();
        allAdminUnits.forEach(adminUnit -> adminUnitNamesCache.put(adminUnit.id(), adminUnit.name()));

        return new AdminUnitsCache(adminUnitNamesCache, countries, counties, localities, groupOfStopPlaces);
    }

    private static boolean isCurrent(TopographicPlace topographicPlace) {
        ValidBetween validBetween = null;
        if (!topographicPlace.getValidBetween().isEmpty()) {
            validBetween = topographicPlace.getValidBetween().get(0);
        }
        if (validBetween == null) {
            return false;
        }
        var fromDate = validBetween.getFromDate();
        var toDate = validBetween.getToDate();
        if (fromDate != null && toDate != null && fromDate.isAfter(toDate)) {
            // Invalid Validity toDate < fromDate
            return false;
        } else return fromDate != null && toDate == null;
    }

    public String getAdminUnitNameForId(String id) {
        return adminUnitNamesCache.getIfPresent(id);
    }

    public AdminUnit getCountryForCountryRef(String countryRef) {
        return countries.values().stream()
                .filter(country -> country.countryRef() != null)
                .filter(adminUnit -> adminUnit.countryRef().equals(countryRef))
                .findFirst().orElse(null);
    }

    public AdminUnit getLocalityForPoint(Point point) {
        return getAdminUnitForGivenPoint(point, localities.values());
    }

    public AdminUnit getCountyForPoint(Point point) {
        return getAdminUnitForGivenPoint(point, counties.values());
    }

    public AdminUnit getCountryForPoint(Point point) {
        return getAdminUnitForGivenPoint(point, countries.values());
    }

    private static AdminUnit getAdminUnitForGivenPoint(Point point, Collection<AdminUnit> topographicPlaces) {
        if (topographicPlaces == null) {
            return null;
        }
        for (var topographicPlace : topographicPlaces) {
            var polygon = topographicPlace.geometry();
            if (polygon != null && polygon.covers(point)) {
                return topographicPlace;
            }
        }
        return null;
    }
}
