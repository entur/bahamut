/*
 * Licensed under the EUPL, Version 1.2 or – as soon they will be approved by
 * the European Commission - subsequent versions of the EUPL (the "Licence");
 * You may not use this work except in compliance with the Licence.
 * You may obtain a copy of the Licence at:
 *
 *   https://joinup.ec.europa.eu/software/page/eupl
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the Licence is distributed on an "AS IS" basis,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Licence for the specific language governing permissions and
 * limitations under the Licence.
 *
 */

package org.entur.bahamut.peliasDocument.stopPlacestoPeliasDocument;

import org.entur.bahamut.adminUnitsCache.AdminUnit;
import org.entur.bahamut.adminUnitsCache.AdminUnitsCache;
import org.entur.bahamut.peliasDocument.model.GeoPoint;
import org.entur.bahamut.peliasDocument.model.Parent;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;

public class Parents {

    public static Parent createParentForTopographicPlaceRef(String topographicPlaceRef,
                                                            GeoPoint centerPoint,
                                                            AdminUnitsCache adminUnitsCache) {
        if (topographicPlaceRef != null) {

            // Try getting parent info for locality by id.
            var locality = adminUnitsCache.localities().get(topographicPlaceRef);
            if (locality != null) {
                return createParentForLocality(locality, adminUnitsCache);
            }

            // Try getting parent info for locality by reverse geocoding.
            locality = createParentByReverseGeocoding(centerPoint, adminUnitsCache, Parent.FieldName.LOCALITY);
            if (locality != null) {
                return createParentForLocality(locality, adminUnitsCache);
            }

            // Try getting parent info for county by id.
            var county = adminUnitsCache.counties().get(topographicPlaceRef);
            if (county != null) {
                return createParentForCounty(county, adminUnitsCache);
            }

            // Try getting parent info for county by reverse geocoding.
            county = createParentByReverseGeocoding(centerPoint, adminUnitsCache, Parent.FieldName.COUNTY);
            if (county != null) {
                return createParentForCounty(county, adminUnitsCache);
            }

            // Try getting parent info for country by id.
            var country = adminUnitsCache.countries().get(topographicPlaceRef);
            if (country != null) {
                return createParentForCountry(country);
            }

            // Try getting parent info for country by reverse geocoding.
            country = createParentByReverseGeocoding(centerPoint, adminUnitsCache, Parent.FieldName.COUNTRY);
            if (country != null) {
                return createParentForCountry(country);
            }
        } else {
            var locality = createParentByReverseGeocoding(centerPoint, adminUnitsCache, Parent.FieldName.LOCALITY);
            if (locality != null) {
                return createParentForLocality(locality, adminUnitsCache);
            }

            var county = createParentByReverseGeocoding(centerPoint, adminUnitsCache, Parent.FieldName.COUNTY);
            if (county != null) {
                return createParentForCounty(county, adminUnitsCache);
            }

            var country = createParentByReverseGeocoding(centerPoint, adminUnitsCache, Parent.FieldName.COUNTRY);
            if (country != null) {
                return createParentForCountry(country);
            }
        }
        return null;
    }

    private static Parent createParentForLocality(AdminUnit locality, AdminUnitsCache adminUnitsCache) {
        var parent = Parent.initParentWithField(
                Parent.FieldName.LOCALITY,
                new Parent.Field(locality.id(), locality.name())
        );

        var countyForLocality = adminUnitsCache.counties().get(locality.parentId());
        if (countyForLocality != null) {
            parent.addOrReplaceParentField(
                    Parent.FieldName.COUNTY,
                    new Parent.Field(countyForLocality.id(), countyForLocality.name())
            );
        }

        var countryForLocality = adminUnitsCache.getCountryForCountryRef(locality.countryRef());
        if (countryForLocality != null) {
            parent.addOrReplaceParentField(
                    Parent.FieldName.COUNTRY,
                    new Parent.Field(countryForLocality.id(), countryForLocality.name(), countryForLocality.getISO3CountryName())
            );
        } else if (locality.countryRef().equalsIgnoreCase("no")) {
            // TODO: Remove this when Assad adds Norway as TopographicPlace i NSR netex file.
            parent.addOrReplaceParentField(
                    Parent.FieldName.COUNTRY,
                    new Parent.Field("FAKE-ID", "Norway", "NOR")
            );
        }

        return parent;
    }

    private static Parent createParentForCounty(AdminUnit county, AdminUnitsCache adminUnitsCache) {
        var parent = Parent.initParentWithField(
                Parent.FieldName.COUNTY,
                new Parent.Field(county.id(), county.name())
        );

        var countryForLocality = adminUnitsCache.getCountryForCountryRef(county.countryRef());
        if (countryForLocality != null) {
            parent.addOrReplaceParentField(
                    Parent.FieldName.COUNTRY,
                    new Parent.Field(countryForLocality.id(), countryForLocality.name(), countryForLocality.getISO3CountryName())
            );
        }
        return parent;
    }

    private static Parent createParentForCountry(AdminUnit country) {
        return Parent.initParentWithField(
                Parent.FieldName.COUNTRY,
                new Parent.Field(country.id(), country.name(), country.getISO3CountryName())
        );
    }

    private static AdminUnit createParentByReverseGeocoding(GeoPoint centerPoint,
                                                            AdminUnitsCache adminUnitsCache,
                                                            Parent.FieldName parentField) {
        var geometryFactory = new GeometryFactory();
        var point = geometryFactory.createPoint(new Coordinate(centerPoint.lon(), centerPoint.lat()));

        return switch (parentField) {
            case LOCALITY -> adminUnitsCache.getLocalityForPoint(point);
            case COUNTY -> adminUnitsCache.getCountyForPoint(point);
            case COUNTRY -> adminUnitsCache.getCountryForPoint(point);
        };
    }
}