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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.io.IOException;
import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class StopPlaceBoostConfigJSON {

    public long defaultValue;

    public Map<String, Double> interchangeFactors;

    public Map<String, Map<String, Double>> stopTypeFactors;

    public static StopPlaceBoostConfigJSON fromString(String string) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            mapper.registerModule(new JavaTimeModule());
            return mapper.readValue(string, StopPlaceBoostConfigJSON.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
