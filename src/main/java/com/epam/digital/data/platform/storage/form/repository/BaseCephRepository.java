/*
 * Copyright 2021 EPAM Systems.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.epam.digital.data.platform.storage.form.repository;

import com.epam.digital.data.platform.integration.ceph.exception.CephCommunicationException;
import com.epam.digital.data.platform.integration.ceph.exception.MisconfigurationException;
import com.epam.digital.data.platform.storage.form.exception.FormDataRepositoryCommunicationException;
import com.epam.digital.data.platform.storage.form.exception.FormDataRepositoryMisconfigurationException;
import lombok.extern.slf4j.Slf4j;

import java.util.function.Supplier;

@Slf4j
public abstract class BaseCephRepository {

    protected  <T> T execute(Supplier<T> supplier) {
        try {
            return supplier.get();
        } catch (CephCommunicationException ex) {
            log.warn("Couldn't get form data from ceph", ex);
            throw new FormDataRepositoryCommunicationException(ex.getMessage(), ex);
        } catch (MisconfigurationException ex) {
            log.warn("Ceph configuration error", ex);
            throw new FormDataRepositoryMisconfigurationException(ex.getMessage(), ex);
        }
    }

    protected void execute(Runnable runnable) {
        try {
            runnable.run();
        } catch (CephCommunicationException ex) {
            log.warn("Couldn't get form data from ceph", ex);
            throw new FormDataRepositoryCommunicationException(ex.getMessage(), ex);
        } catch (MisconfigurationException ex) {
            log.warn("Ceph configuration error", ex);
            throw new FormDataRepositoryMisconfigurationException(ex.getMessage(), ex);
        }
    }
}
