/*
 *  Copyright Nomura Research Institute, Ltd.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package jp.openstandia.connector.smarthr;

import jp.openstandia.connector.smarthr.testutil.AbstractTest;
import org.identityconnectors.framework.common.objects.ObjectClassInfo;
import org.identityconnectors.framework.common.objects.Schema;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class SchemaTest extends AbstractTest {

    @Test
    void schema() {
        Schema schema = connector.schema();

        assertNotNull(schema);
        assertEquals(6, schema.getObjectClassInfo().size());

        Optional<ObjectClassInfo> crew = schema.getObjectClassInfo().stream().filter(o -> o.is("crew")).findFirst();
        Optional<ObjectClassInfo> department = schema.getObjectClassInfo().stream().filter(o -> o.is("department")).findFirst();
        Optional<ObjectClassInfo> employmentType = schema.getObjectClassInfo().stream().filter(o -> o.is("employment_type")).findFirst();
        Optional<ObjectClassInfo> jobTitle = schema.getObjectClassInfo().stream().filter(o -> o.is("job_title")).findFirst();
        Optional<ObjectClassInfo> company = schema.getObjectClassInfo().stream().filter(o -> o.is("company")).findFirst();
        Optional<ObjectClassInfo> bizEstablishment = schema.getObjectClassInfo().stream().filter(o -> o.is("biz_establishment")).findFirst();

        assertTrue(crew.isPresent());
        assertTrue(department.isPresent());
        assertTrue(employmentType.isPresent());
        assertTrue(jobTitle.isPresent());
        assertTrue(company.isPresent());
        assertTrue(bizEstablishment.isPresent());
    }
}
