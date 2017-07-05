package org.radarcns.util;

import java.io.IOException;
import org.bson.Document;

/*
 * Copyright 2017 King's College London and The Hyve
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
public abstract class SenderTestCase extends TestCase {

    public abstract String getTopicName();

    public abstract void send()
            throws IOException, IllegalAccessException, InstantiationException;

    public abstract Document getExpectedDocument();

}
