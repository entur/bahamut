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

package org.entur.bahamut.camel.routes;

import org.entur.bahamut.camel.routes.json.PeliasDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class PeliasIndexValidCommandFilter {
    private static final Logger logger = LoggerFactory.getLogger(PeliasIndexValidCommandFilter.class);

    /**
     * Remove invalid indexing commands.
     * Certain commands will be acceptable for insert into Elasticsearch, but will cause Pelias API to fail upon subsequent queries.
     */
    public static boolean isValid(ElasticsearchCommand command) {
        if (command == null || command.getIndex() == null) {
            logger.warn("Removing invalid command");
            return false;
        }
        if (command.getIndex().getIndex() == null || command.getIndex().getType() == null) {
            logger.warn("Removing invalid command with missing index name or type:" + command);
            return false;
        }

        if (!(command.getSource() instanceof PeliasDocument doc)) {
            logger.warn("Removing invalid command with missing pelias document:" + command);
            return false;
        }

        if (doc.getLayer() == null || doc.getSource() == null || doc.getSourceId() == null) {
            logger.warn("Removing invalid command where pelias document is missing mandatory fields:" + command);
            return false;
        }

        if (doc.getCenterPoint() == null) {
            logger.debug("Removing invalid command where geometry is missing:" + command);
            return false;
        }
        return true;
    }
}
