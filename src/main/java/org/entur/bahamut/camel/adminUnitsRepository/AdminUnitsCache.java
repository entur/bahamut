package org.entur.bahamut.camel.adminUnitsRepository;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.entur.netex.index.api.NetexEntitiesIndex;
import org.locationtech.jts.geom.Point;
import org.rutebanken.netex.model.GroupOfStopPlaces;
import org.rutebanken.netex.model.IanaCountryTldEnumeration;
import org.rutebanken.netex.model.TopographicPlace;
import org.rutebanken.netex.model.ValidBetween;

import java.time.LocalDateTime;
import java.util.List;

public record AdminUnitsCache(Cache<String, String> adminUnitNamesCache,
                              List<AdminUnit> localities,
                              List<AdminUnit> countries,
                              List<GroupOfStopPlaces> groupOfStopPlaces) {
    public static AdminUnitsCache buildNewCache(NetexEntitiesIndex netexEntitiesIndex, Integer cacheMaxSize) {

        List<GroupOfStopPlaces> groupOfStopPlaces = netexEntitiesIndex.getSiteFrames().stream()
                .flatMap(siteFrame -> siteFrame.getGroupsOfStopPlaces().getGroupOfStopPlaces().stream())
                .toList();

        List<AdminUnit> allAdminUnits = netexEntitiesIndex.getSiteFrames().stream()
                .flatMap(siteFrame -> siteFrame.getTopographicPlaces().getTopographicPlace().stream())
                .filter(AdminUnitsCache::isCurrent)
                .filter(topographicPlace -> {
                    LocalDateTime toDate = topographicPlace.getValidBetween().get(0).getToDate();
                    return toDate == null || toDate.isAfter(LocalDateTime.now());
                })
                .filter(topographicPlace -> topographicPlace.getPolygon() != null)
                .map(AdminUnit::makeAdminUnit)
                .toList();

        List<AdminUnit> localities = allAdminUnits.stream()
                .filter(adminUnit -> adminUnit.adminUnitType() == AdminUnitType.LOCALITY).toList();

        List<AdminUnit> countries = allAdminUnits.stream()
                .filter(adminUnit -> adminUnit.adminUnitType() == AdminUnitType.COUNTRY)
                .filter(adminUnit -> !adminUnit.countryRef().equals(IanaCountryTldEnumeration.RU.name()))
                .toList();

        Cache<String, String> idCache = CacheBuilder.newBuilder().maximumSize(cacheMaxSize).build();
        allAdminUnits.stream()
                .filter(adminUnit -> adminUnit.adminUnitType() == AdminUnitType.LOCALITY || adminUnit.adminUnitType() == AdminUnitType.COUNTY)
                .forEach(adminUnit -> idCache.put(adminUnit.id(), adminUnit.name()));

        return new AdminUnitsCache(idCache, localities, countries, groupOfStopPlaces);
    }

    private static boolean isCurrent(TopographicPlace topographicPlace) {
        ValidBetween validBetween = null;
        if (!topographicPlace.getValidBetween().isEmpty()) {
            validBetween = topographicPlace.getValidBetween().get(0);
        }
        if (validBetween == null) {
            return false;
        }
        final LocalDateTime fromDate = validBetween.getFromDate();
        final LocalDateTime toDate = validBetween.getToDate();
        if (fromDate != null && toDate != null && fromDate.isAfter(toDate)) {
            // Invalid Validity toDate < fromDate
            return false;
        } else return fromDate != null && toDate == null;
    }

    public String getAdminUnitNameForId(String id) {
        return adminUnitNamesCache.getIfPresent(id);
    }

    public AdminUnit getLocalityForId(String id) {
        return localities.stream()
                .filter(adminUnit -> adminUnit.id() != null)
                .filter(adminUnit -> adminUnit.id().equals(id))
                .findFirst().orElse(null);
    }

    public AdminUnit getLocalityForPoint(Point point) {
        return getAdminUnitForGivenPoint(point, localities);
    }

    public AdminUnit getCountryForPoint(Point point) {
        return getAdminUnitForGivenPoint(point, countries);
    }

    private static AdminUnit getAdminUnitForGivenPoint(Point point, List<AdminUnit> topographicPlaces) {
        if (topographicPlaces == null) {
            return null;
        }

        for (AdminUnit topographicPlace : topographicPlaces) {
            var polygon = topographicPlace.geometry();
            if (polygon != null && polygon.covers(point)) {
                return topographicPlace;
            }
        }
        return null;
    }
}
