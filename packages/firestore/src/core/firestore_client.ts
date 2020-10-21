/**
 * @license
 * Copyright 2017 Google LLC
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

import { GetOptions } from '@firebase/firestore-types';

import {
  CredentialChangeListener,
  CredentialsProvider
} from '../api/credentials';
import { User } from '../auth/user';
import {
  executeQuery,
  LocalStore,
  readLocalDocument
} from '../local/local_store';
import { Document, NoDocument } from '../model/document';
import { DocumentKey } from '../model/document_key';
import { Mutation } from '../model/mutation';
import {
  remoteStoreEnableNetwork,
  remoteStoreDisableNetwork
} from '../remote/remote_store';
import { AsyncQueue, wrapInUserErrorIfRecoverable } from '../util/async_queue';
import { Code, FirestoreError } from '../util/error';
import { Deferred } from '../util/promise';
import {
  addSnapshotsInSyncListener,
  EventManager,
  eventManagerListen,
  eventManagerUnlisten,
  ListenOptions,
  Observer,
  QueryListener,
  removeSnapshotsInSyncListener
} from './event_manager';
import { registerPendingWritesCallback, syncEngineWrite } from './sync_engine';
import { View } from './view';
import { DatabaseId, DatabaseInfo } from './database_info';
import { newQueryForPath, Query } from './query';
import { Transaction } from './transaction';
import { ViewSnapshot } from './view_snapshot';
import { ComponentConfiguration } from './component_provider';
import { AsyncObserver } from '../util/async_observer';
import { debugAssert } from '../util/assert';
import { TransactionRunner } from './transaction_runner';
import {
  getDatastore,
  getEventManager,
  getLocalStore,
  getPersistence,
  getRemoteStore,
  getSyncEngine,
  removeComponents
} from '../../exp/src/api/components';
import { logDebug } from '../util/log';
import { AutoId } from '../util/misc';

const LOG_TAG = 'FirestoreClient';
export const MAX_CONCURRENT_LIMBO_RESOLUTIONS = 100;

/**
 * FirestoreClient is a top-level class that constructs and owns all of the
 * pieces of the client SDK architecture. It is responsible for creating the
 * async queue that is shared by all of the other components in the system.
 */
export class FirestoreClient {
  // NOTE: These should technically have '|undefined' in the types, since
  // they're initialized asynchronously rather than in the constructor, but
  // given that all work is done on the async queue and we assert that
  // initialization completes before any other work is queued, we're cheating
  // with the types rather than littering the code with '!' or unnecessary
  // undefined checks.
  private databaseInfo!: DatabaseInfo;
  private user = User.UNAUTHENTICATED;
  private readonly clientId = AutoId.newId();
  private _credentialListener: CredentialChangeListener = () => {};

  // We defer our initialization until we get the current user from
  // setChangeListener(). We block the async queue until we got the initial
  // user and the initialization is completed. This will prevent any scheduled
  // work from happening before initialization is completed.
  //
  // If initializationDone resolved then the FirestoreClient is in a usable
  // state.
  private readonly initializationDone = new Deferred<void>();

  constructor(
    private credentials: CredentialsProvider,
    /**
     * Asynchronous queue responsible for all of our internal processing. When
     * we get incoming work from the user (via public API) or the network
     * (incoming GRPC messages), we should always schedule onto this queue.
     * This ensures all of our work is properly serialized (e.g. we don't
     * start processing a new operation while the previous one is waiting for
     * an async I/O to complete).
     */
    public asyncQueue: AsyncQueue
  ) {}

  /**
   * Starts up the FirestoreClient, returning only whether or not enabling
   * persistence succeeded.
   *
   * The intent here is to "do the right thing" as far as users are concerned.
   * Namely, in cases where offline persistence is requested and possible,
   * enable it, but otherwise fall back to persistence disabled. For the most
   * part we expect this to succeed one way or the other so we don't expect our
   * users to actually wait on the firestore.enablePersistence Promise since
   * they generally won't care.
   *
   * Of course some users actually do care about whether or not persistence
   * was successfully enabled, so the Promise returned from this method
   * indicates this outcome.
   *
   * This presents a problem though: even before enablePersistence resolves or
   * rejects, users may have made calls to e.g. firestore.collection() which
   * means that the FirestoreClient in there will be available and will be
   * enqueuing actions on the async queue.
   *
   * Meanwhile any failure of an operation on the async queue causes it to
   * panic and reject any further work, on the premise that unhandled errors
   * are fatal.
   *
   * Consequently the fallback is handled internally here in start, and if the
   * fallback succeeds we signal success to the async queue even though the
   * start() itself signals failure.
   *
   * @param databaseInfo The connection information for the current instance.
   */
  start(databaseInfo: DatabaseInfo): void {
    this.databaseInfo = databaseInfo;

    let initialized = false;
    this.credentials.setChangeListener(user => {
      if (!initialized) {
        initialized = true;

        logDebug(LOG_TAG, 'Initializing. user=', user.uid);
        this.initializationDone.resolve();
      }
      if (!user.isEqual(this.user)) {
        this.user = user;
        this._credentialListener(user);
      }
    });

    // Block the async queue until initialization is done
    this.asyncQueue.enqueueAndForget(() => this.initializationDone.promise);
  }

  async getConfiguration(): Promise<ComponentConfiguration> {
    await this.initializationDone.promise;

    return {
      asyncQueue: this.asyncQueue,
      databaseInfo: this.databaseInfo,
      clientId: this.clientId,
      credentials: this.credentials,
      initialUser: this.user,
      maxConcurrentLimboResolutions: MAX_CONCURRENT_LIMBO_RESOLUTIONS
    };
  }

  setCredentialChangeListener(listener: (user: User) => void): void {
    logDebug('FirebaseFirestore', 'Registering credential change listener');
    this._credentialListener = listener;
    // eslint-disable-next-line @typescript-eslint/no-floating-promises
    this.initializationDone.promise.then(() =>
      this._credentialListener(this.user)
    );
  }

  /**
   * Checks that the client has not been terminated. Ensures that other methods on
   * this class cannot be called after the client is terminated.
   */
  verifyNotTerminated(): void {
    if (this.asyncQueue.isShuttingDown) {
      throw new FirestoreError(
        Code.FAILED_PRECONDITION,
        'The client has already been terminated.'
      );
    }
  }

  databaseId(): DatabaseId {
    return this.databaseInfo.databaseId;
  }

  terminate(): Promise<void> {
    this.asyncQueue.enterRestrictedMode();
    const deferred = new Deferred();
    this.asyncQueue.enqueueAndForgetEvenWhileRestricted(async () => {
      try {
        await removeComponents(this);

        // `removeChangeListener` must be called after shutting down the
        // RemoteStore as it will prevent the RemoteStore from retrieving
        // auth tokens.
        this.credentials.removeChangeListener();
        deferred.resolve();
      } catch (e) {
        const firestoreError = wrapInUserErrorIfRecoverable(
          e,
          `Failed to shutdown persistence`
        );
        deferred.reject(firestoreError);
      }
    });
    return deferred.promise;
  }
}

/** Enables the network connection and requeues all pending operations. */
export async function firestoreClientEnableNetwork(
  firestoreClient: FirestoreClient
): Promise<void> {
  firestoreClient.verifyNotTerminated();
  return firestoreClient.asyncQueue.enqueue(async () => {
    const persistence = await getPersistence(firestoreClient);
    const remoteStore = await getRemoteStore(firestoreClient);
    persistence.setNetworkEnabled(true);
    return remoteStoreEnableNetwork(remoteStore);
  });
}

/** Disables the network connection. Pending operations will not complete. */
export async function firestoreClientDisableNetwork(
  firestoreClient: FirestoreClient
): Promise<void> {
  firestoreClient.verifyNotTerminated();
  return firestoreClient.asyncQueue.enqueue(async () => {
    const persistence = await getPersistence(firestoreClient);
    const remoteStore = await getRemoteStore(firestoreClient);
    persistence.setNetworkEnabled(false);
    return remoteStoreDisableNetwork(remoteStore);
  });
}

/**
 * Returns a Promise that resolves when all writes that were pending at the time
 * this method was called received server acknowledgement. An acknowledgement
 * can be either acceptance or rejection.
 */
export async function firestoreClientWaitForPendingWrites(
  firestoreClient: FirestoreClient
): Promise<void> {
  firestoreClient.verifyNotTerminated();

  const deferred = new Deferred<void>();
  firestoreClient.asyncQueue.enqueueAndForget(async () => {
    const syncEngine = await getSyncEngine(firestoreClient);
    return registerPendingWritesCallback(syncEngine, deferred);
  });
  return deferred.promise;
}

export function firestoreClientListen(
  firestoreClient: FirestoreClient,
  query: Query,
  options: ListenOptions,
  observer: Partial<Observer<ViewSnapshot>>
): () => void {
  firestoreClient.verifyNotTerminated();
  const wrappedObserver = new AsyncObserver(observer);
  const listener = new QueryListener(query, wrappedObserver, options);
  firestoreClient.asyncQueue.enqueueAndForget(async () => {
    const eventManager = await getEventManager(firestoreClient);
    return eventManagerListen(eventManager, listener);
  });
  return () => {
    wrappedObserver.mute();
    firestoreClient.asyncQueue.enqueueAndForget(async () => {
      const eventManager = await getEventManager(firestoreClient);
      return eventManagerUnlisten(eventManager, listener);
    });
  };
}

export function firestoreClientGetDocumentFromLocalCache(
  firestoreClient: FirestoreClient,
  docKey: DocumentKey
): Promise<Document | null> {
  firestoreClient.verifyNotTerminated();
  const deferred = new Deferred<Document | null>();
  firestoreClient.asyncQueue.enqueueAndForget(async () => {
    const localStore = await getLocalStore(firestoreClient);
    return readDocumentFromCache(localStore, docKey, deferred);
  });
  return deferred.promise;
}

export function firestoreClientGetDocumentViaSnapshotListener(
  firestoreClient: FirestoreClient,
  key: DocumentKey,
  options: GetOptions = {}
): Promise<ViewSnapshot> {
  firestoreClient.verifyNotTerminated();
  const deferred = new Deferred<ViewSnapshot>();
  firestoreClient.asyncQueue.enqueueAndForget(async () => {
    const eventManager = await getEventManager(firestoreClient);
    return readDocumentViaSnapshotListener(
      eventManager,
      firestoreClient.asyncQueue,
      key,
      options,
      deferred
    );
  });
  return deferred.promise;
}

export function firestoreClientGetDocumentsFromLocalCache(
  firestoreClient: FirestoreClient,
  query: Query
): Promise<ViewSnapshot> {
  firestoreClient.verifyNotTerminated();
  const deferred = new Deferred<ViewSnapshot>();
  firestoreClient.asyncQueue.enqueueAndForget(async () => {
    const localStore = await getLocalStore(firestoreClient);
    return executeQueryFromCache(localStore, query, deferred);
  });
  return deferred.promise;
}

export function firestoreClientGetDocumentsViaSnapshotListener(
  firestoreClient: FirestoreClient,
  query: Query,
  options: GetOptions = {}
): Promise<ViewSnapshot> {
  firestoreClient.verifyNotTerminated();
  const deferred = new Deferred<ViewSnapshot>();
  firestoreClient.asyncQueue.enqueueAndForget(async () => {
    const eventManager = await getEventManager(firestoreClient);
    return executeQueryViaSnapshotListener(
      eventManager,
      firestoreClient.asyncQueue,
      query,
      options,
      deferred
    );
  });
  return deferred.promise;
}

export function firestoreClientWrite(
  firestoreClient: FirestoreClient,
  mutations: Mutation[]
): Promise<void> {
  firestoreClient.verifyNotTerminated();
  const deferred = new Deferred<void>();
  firestoreClient.asyncQueue.enqueueAndForget(async () => {
    const syncEngine = await getSyncEngine(firestoreClient);
    return syncEngineWrite(syncEngine, mutations, deferred);
  });
  return deferred.promise;
}

export function firestoreClientAddSnapshotsInSyncListener(
  firestoreClient: FirestoreClient,
  observer: Partial<Observer<void>>
): () => void {
  firestoreClient.verifyNotTerminated();
  const wrappedObserver = new AsyncObserver(observer);
  firestoreClient.asyncQueue.enqueueAndForget(async () => {
    const eventManager = await getEventManager(firestoreClient);
    return addSnapshotsInSyncListener(eventManager, wrappedObserver);
  });
  return () => {
    wrappedObserver.mute();
    firestoreClient.asyncQueue.enqueueAndForget(async () => {
      const eventManager = await getEventManager(firestoreClient);
      return removeSnapshotsInSyncListener(eventManager, wrappedObserver);
    });
  };
}

/**
 * Takes an updateFunction in which a set of reads and writes can be performed
 * atomically. In the updateFunction, the client can read and write values
 * using the supplied transaction object. After the updateFunction, all
 * changes will be committed. If a retryable error occurs (ex: some other
 * client has changed any of the data referenced), then the updateFunction
 * will be called again after a backoff. If the updateFunction still fails
 * after all retries, then the transaction will be rejected.
 *
 * The transaction object passed to the updateFunction contains methods for
 * accessing documents and collections. Unlike other datastore access, data
 * accessed with the transaction will not reflect local changes that have not
 * been committed. For this reason, it is required that all reads are
 * performed before any writes. Transactions must be performed while online.
 */
export function firestoreClientTransaction<T>(
  firestoreClient: FirestoreClient,
  updateFunction: (transaction: Transaction) => Promise<T>
): Promise<T> {
  firestoreClient.verifyNotTerminated();
  const deferred = new Deferred<T>();
  firestoreClient.asyncQueue.enqueueAndForget(async () => {
    const datastore = await getDatastore(firestoreClient);
    new TransactionRunner<T>(
      firestoreClient.asyncQueue,
      datastore,
      updateFunction,
      deferred
    ).run();
  });
  return deferred.promise;
}

export async function readDocumentFromCache(
  localStore: LocalStore,
  docKey: DocumentKey,
  result: Deferred<Document | null>
): Promise<void> {
  try {
    const maybeDoc = await readLocalDocument(localStore, docKey);
    if (maybeDoc instanceof Document) {
      result.resolve(maybeDoc);
    } else if (maybeDoc instanceof NoDocument) {
      result.resolve(null);
    } else {
      result.reject(
        new FirestoreError(
          Code.UNAVAILABLE,
          'Failed to get document from cache. (However, this document may ' +
            "exist on the server. Run again without setting 'source' in " +
            'the GetOptions to attempt to retrieve the document from the ' +
            'server.)'
        )
      );
    }
  } catch (e) {
    const firestoreError = wrapInUserErrorIfRecoverable(
      e,
      `Failed to get document '${docKey} from cache`
    );
    result.reject(firestoreError);
  }
}

/**
 * Retrieves a latency-compensated document from the backend via a
 * SnapshotListener.
 */
export function readDocumentViaSnapshotListener(
  eventManager: EventManager,
  asyncQueue: AsyncQueue,
  key: DocumentKey,
  options: GetOptions,
  result: Deferred<ViewSnapshot>
): Promise<void> {
  const wrappedObserver = new AsyncObserver({
    next: (snap: ViewSnapshot) => {
      // Remove query first before passing event to user to avoid
      // user actions affecting the now stale query.
      asyncQueue.enqueueAndForget(() =>
        eventManagerUnlisten(eventManager, listener)
      );

      const exists = snap.docs.has(key);
      if (!exists && snap.fromCache) {
        // TODO(dimond): If we're online and the document doesn't
        // exist then we resolve with a doc.exists set to false. If
        // we're offline however, we reject the Promise in this
        // case. Two options: 1) Cache the negative response from
        // the server so we can deliver that even when you're
        // offline 2) Actually reject the Promise in the online case
        // if the document doesn't exist.
        result.reject(
          new FirestoreError(
            Code.UNAVAILABLE,
            'Failed to get document because the client is offline.'
          )
        );
      } else if (
        exists &&
        snap.fromCache &&
        options &&
        options.source === 'server'
      ) {
        result.reject(
          new FirestoreError(
            Code.UNAVAILABLE,
            'Failed to get document from server. (However, this ' +
              'document does exist in the local cache. Run again ' +
              'without setting source to "server" to ' +
              'retrieve the cached document.)'
          )
        );
      } else {
        debugAssert(
          snap.docs.size <= 1,
          'Expected zero or a single result on a document-only query'
        );
        result.resolve(snap);
      }
    },
    error: e => result.reject(e)
  });

  const listener = new QueryListener(
    newQueryForPath(key.path),
    wrappedObserver,
    {
      includeMetadataChanges: true,
      waitForSyncWhenOnline: true
    }
  );
  return eventManagerListen(eventManager, listener);
}

export async function executeQueryFromCache(
  localStore: LocalStore,
  query: Query,
  result: Deferred<ViewSnapshot>
): Promise<void> {
  try {
    const queryResult = await executeQuery(
      localStore,
      query,
      /* usePreviousResults= */ true
    );
    const view = new View(query, queryResult.remoteKeys);
    const viewDocChanges = view.computeDocChanges(queryResult.documents);
    const viewChange = view.applyChanges(
      viewDocChanges,
      /* updateLimboDocuments= */ false
    );
    result.resolve(viewChange.snapshot!);
  } catch (e) {
    const firestoreError = wrapInUserErrorIfRecoverable(
      e,
      `Failed to execute query '${query} against cache`
    );
    result.reject(firestoreError);
  }
}

/**
 * Retrieves a latency-compensated query snapshot from the backend via a
 * SnapshotListener.
 */
export function executeQueryViaSnapshotListener(
  eventManager: EventManager,
  asyncQueue: AsyncQueue,
  query: Query,
  options: GetOptions,
  result: Deferred<ViewSnapshot>
): Promise<void> {
  const wrappedObserver = new AsyncObserver<ViewSnapshot>({
    next: snapshot => {
      // Remove query first before passing event to user to avoid
      // user actions affecting the now stale query.
      asyncQueue.enqueueAndForget(() =>
        eventManagerUnlisten(eventManager, listener)
      );

      if (snapshot.fromCache && options.source === 'server') {
        result.reject(
          new FirestoreError(
            Code.UNAVAILABLE,
            'Failed to get documents from server. (However, these ' +
              'documents may exist in the local cache. Run again ' +
              'without setting source to "server" to ' +
              'retrieve the cached documents.)'
          )
        );
      } else {
        result.resolve(snapshot);
      }
    },
    error: e => result.reject(e)
  });

  const listener = new QueryListener(query, wrappedObserver, {
    includeMetadataChanges: true,
    waitForSyncWhenOnline: true
  });
  return eventManagerListen(eventManager, listener);
}
