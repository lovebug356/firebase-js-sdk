/**
 * @license
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { FirebaseFirestore, FirestoreCompat } from './database';
import {
  MemoryOfflineComponentProvider,
  OfflineComponentProvider,
  OnlineComponentProvider
} from '../../../src/core/component_provider';
import { handleUserChange, LocalStore } from '../../../src/local/local_store';
import { logDebug } from '../../../src/util/log';
import {
  RemoteStore,
  remoteStoreHandleCredentialChange
} from '../../../src/remote/remote_store';
import {
  SyncEngine,
  syncEngineListen,
  syncEngineUnlisten
} from '../../../src/core/sync_engine';
import { Persistence } from '../../../src/local/persistence';
import { EventManager } from '../../../src/core/event_manager';
import { FirestoreClient } from '../../../src/core/firestore_client';
import { Datastore } from '../../../src/remote/datastore';

const LOG_TAG = 'ComponentProvider';

// The components module manages the lifetime of dependencies of the Firestore
// client. Dependencies can be lazily constructed and only one exists per
// Firestore instance.

// Instance maps that ensure that only one component provider exists per
// Firestore instance.
const offlineComponentProviders = new Map<
  FirestoreClient,
  OfflineComponentProvider
>();
const onlineComponentProviders = new Map<
  FirestoreClient,
  OnlineComponentProvider
>();

export async function setOfflineComponentProvider(
  firestore: FirestoreCompat,
  offlineComponentProvider: OfflineComponentProvider
): Promise<void> {
  firestore._queue.verifyOperationInProgress();
  const firestoreClient = firestore._firestoreClient;

  logDebug(LOG_TAG, 'Initializing OfflineComponentProvider');
  const configuration = await firestoreClient.getConfiguration();
  await offlineComponentProvider.initialize(configuration);
  firestoreClient.setCredentialChangeListener(user =>
    firestoreClient.asyncQueue.enqueueRetryable(async () => {
      await handleUserChange(offlineComponentProvider.localStore, user);
    })
  );
  // When a user calls clearPersistence() in one client, all other clients
  // need to be terminated to allow the delete to succeed.
  offlineComponentProvider.persistence.setDatabaseDeletedListener(() =>
    firestore._delete()
  );

  offlineComponentProviders.set(firestoreClient, offlineComponentProvider);
}

export async function setOnlineComponentProvider(
  firestore: FirestoreCompat,
  onlineComponentProvider: OnlineComponentProvider
): Promise<void> {
  firestore._queue.verifyOperationInProgress();
  const firestoreClient = firestore._firestoreClient;

  const offlineComponentProvider = await getOfflineComponentProvider(firestore);

  logDebug(LOG_TAG, 'Initializing OnlineComponentProvider');
  const configuration = await firestoreClient.getConfiguration();
  await onlineComponentProvider.initialize(
    offlineComponentProvider,
    configuration
  );
  // The CredentialChangeListener of the online component provider takes
  // precedence over the offline component provider.
  firestoreClient.setCredentialChangeListener(user =>
    firestoreClient.asyncQueue.enqueueRetryable(() =>
      remoteStoreHandleCredentialChange(
        onlineComponentProvider.remoteStore,
        user
      )
    )
  );

  onlineComponentProviders.set(firestoreClient, onlineComponentProvider);
}

// TODO(firestore-compat): Remove `export` once compat migration is complete.
export async function getOfflineComponentProvider(
  firestore: FirestoreCompat
): Promise<OfflineComponentProvider> {
  firestore.asyncQueue.verifyOperationInProgress();
  const firestoreClient = await firestore._firestoreClient;

  if (!offlineComponentProviders.has(firestore)) {
    logDebug(LOG_TAG, 'Using default OfflineComponentProvider');
    await setOfflineComponentProvider(
      firestore,
      new MemoryOfflineComponentProvider()
    );
  }

  return offlineComponentProviders.get(firestore)!;
}

// TODO(firestore-compat): Remove `export` once compat migration is complete.
export async function getOnlineComponentProvider(
  firestore: FirestoreClient
): Promise<OnlineComponentProvider> {
  firestore.asyncQueue.verifyOperationInProgress();

  if (!onlineComponentProviders.has(firestore)) {
    logDebug(LOG_TAG, 'Using default OnlineComponentProvider');
    await setOnlineComponentProvider(firestore, new OnlineComponentProvider());
  }

  return onlineComponentProviders.get(firestore)!;
}

export async function getSyncEngine(
  firestore: FirestoreClient
): Promise<SyncEngine> {
  const onlineComponentProvider = await getOnlineComponentProvider(firestore);
  return onlineComponentProvider.syncEngine;
}

export async function getRemoteStore(
  firestore: FirestoreClient
): Promise<RemoteStore> {
  const onlineComponentProvider = await getOnlineComponentProvider(firestore);
  return onlineComponentProvider.remoteStore;
}

export async function getDatastore(
  firestore: FirestoreClient
): Promise<Datastore> {
  const onlineComponentProvider = await getOnlineComponentProvider(firestore);
  return onlineComponentProvider.datastore;
}

export async function getEventManager(
  firestore: FirestoreClient
): Promise<EventManager> {
  const onlineComponentProvider = await getOnlineComponentProvider(firestore);
  const eventManager = onlineComponentProvider.eventManager;
  eventManager.onListen = syncEngineListen.bind(
    null,
    onlineComponentProvider.syncEngine
  );
  eventManager.onUnlisten = syncEngineUnlisten.bind(
    null,
    onlineComponentProvider.syncEngine
  );
  return eventManager;
}

export async function getPersistence(
  firestore: FirestoreClient
): Promise<Persistence> {
  const offlineComponentProvider = await getOfflineComponentProvider(firestore);
  return offlineComponentProvider.persistence;
}

export async function getLocalStore(
  firestore: FirestoreClient
): Promise<LocalStore> {
  const offlineComponentProvider = await getOfflineComponentProvider(firestore);
  return offlineComponentProvider.localStore;
}

/**
 * Removes all components associated with the provided instance. Must be called
 * when the Firestore instance is terminated.
 */
export async function removeComponents(
  firestore: FirestoreClient
): Promise<void> {
  const onlineComponentProviderPromise = onlineComponentProviders.get(
    firestore
  );
  if (onlineComponentProviderPromise) {
    logDebug(LOG_TAG, 'Removing OnlineComponentProvider');
    onlineComponentProviders.delete(firestore);
    await (await onlineComponentProviderPromise).terminate();
  }

  const offlineComponentProviderPromise = offlineComponentProviders.get(
    firestore
  );
  if (offlineComponentProviderPromise) {
    logDebug(LOG_TAG, 'Removing OfflineComponentProvider');
    offlineComponentProviders.delete(firestore);
    await (await offlineComponentProviderPromise).terminate();
  }
}
