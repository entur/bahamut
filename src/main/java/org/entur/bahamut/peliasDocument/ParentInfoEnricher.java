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

package org.entur.bahamut.peliasDocument;

import org.entur.bahamut.adminUnitsCache.AdminUnitsCache;
import org.entur.bahamut.peliasDocument.model.Parent;
import org.entur.bahamut.peliasDocument.model.PeliasDocument;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class ParentInfoEnricher {

    private static final Logger logger = LoggerFactory.getLogger(ParentInfoEnricher.class);

    /**
     * Enrich peliasDocument with parent info.
     */
    public static void enrichParentInfo(AdminUnitsCache adminUnitsCache, PeliasDocument peliasDocument) {
        var parent = peliasDocument.parent();

        if (parent == null || parent.idFor(Parent.FieldName.LOCALITY).isEmpty()) {
            logger.debug("Locality is missing doing reverseGeoLookup for :" +
                    peliasDocument.category() + " type: " + peliasDocument.layer());
            enrichParentInfoByReverseGeoLookup(adminUnitsCache, peliasDocument);
        }

        // if we were able to add the localityId in the previous step, or it was already there.
        if (parent != null) {
            parent.idFor(Parent.FieldName.LOCALITY).ifPresent(localityId -> {
                logger.debug("LocalityId exists, adding parent info");
                enrichParentInfoByIds(adminUnitsCache, peliasDocument);
            });
        }
    }

    private static void enrichParentInfoByIds(AdminUnitsCache adminUnitsCache, PeliasDocument peliasDocument) {
        var parent = peliasDocument.parent();

        parent.idFor(Parent.FieldName.LOCALITY).ifPresent(localityId -> {
            if (parent.nameFor(Parent.FieldName.LOCALITY).isEmpty()) {
                logger.debug("1. Locality is missing get locality name by id: " + localityId + " type: " + peliasDocument.layer());
                var adminUnitLocality = adminUnitsCache.getLocalityForId(localityId);
                if (adminUnitLocality != null) {
                    parent.setNameFor(Parent.FieldName.LOCALITY, adminUnitLocality.name());
                    parent.addOrReplaceParentField(Parent.FieldName.COUNTY, new Parent.Field(adminUnitLocality.parentId(), null)); // TODO
                    parent.addOrReplaceParentField(Parent.FieldName.COUNTRY, new Parent.Field(adminUnitLocality.getISO3CountryName(), adminUnitLocality.countryRef())); // TODO no country name in netex file.
                } else {
                    // Locality id on document does not match any known locality, match on geography instead
                    logger.debug("Locality is missing doing reverseGeoLookup for :" + peliasDocument.category() + " type: " + peliasDocument.layer());
                    enrichParentInfoByReverseGeoLookup(adminUnitsCache, peliasDocument);

                    logger.debug("2. Locality is still missing ,doing Reverse lookup again:  " + localityId);
                    enrichParentInfoByReverseGeoLookup(adminUnitsCache, peliasDocument);

                    logger.debug("3. Once again setLocality by Id : " + localityId);
                    var adminUnitName = adminUnitsCache.getAdminUnitNameForId(localityId);

                    parent.setNameFor(Parent.FieldName.LOCALITY, adminUnitName);
                }
            }
        });

        parent.idFor(Parent.FieldName.COUNTY).ifPresent(countyId -> {
            if (parent.nameFor(Parent.FieldName.COUNTY).isEmpty()) {
                logger.debug("County is missing get county name by id: " + countyId + " type: " + peliasDocument.layer());
                var adminUnitName = adminUnitsCache.getAdminUnitNameForId(countyId);
                parent.setNameFor(Parent.FieldName.COUNTY, adminUnitName);
            }
        });
    }

    private static void enrichParentInfoByReverseGeoLookup(AdminUnitsCache adminUnitsCache, PeliasDocument peliasDocument) {
        var centerPoint = peliasDocument.centerPoint();
        GeometryFactory geometryFactory = new GeometryFactory();
        var point = geometryFactory.createPoint(new Coordinate(centerPoint.lon(), centerPoint.lat()));

        var adminUnitLocality = adminUnitsCache.getLocalityForPoint(point);
        var adminUnitCountry = adminUnitsCache.getCountryForPoint(point); // TODO: No need to run it, if its not needed
        var parent = peliasDocument.parent();
        if (adminUnitLocality != null) {
            if (parent == null) {
                parent = new Parent();
                peliasDocument.setParent(parent);
            }
            parent.addOrReplaceParentField(Parent.FieldName.LOCALITY, new Parent.Field(adminUnitLocality.id(), null)); // TODO
            parent.addOrReplaceParentField(Parent.FieldName.COUNTY, new Parent.Field(adminUnitLocality.parentId(), null));
            parent.addOrReplaceParentField(Parent.FieldName.COUNTRY, new Parent.Field(adminUnitLocality.countryRef(), null));
        } else if (adminUnitCountry != null) {
            if (parent == null) {
                parent = new Parent();
                peliasDocument.setParent(parent);
            }
            parent.addOrReplaceParentField(Parent.FieldName.COUNTRY, new Parent.Field(adminUnitCountry.countryRef(), null)); // TODO
        }
    }
}
