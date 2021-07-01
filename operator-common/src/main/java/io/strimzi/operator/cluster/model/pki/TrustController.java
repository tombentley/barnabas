/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki;

import java.security.cert.CertificateEncodingException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;

/**
 * <p>Implements the Trust State Machine by consuming a set of trust roots with their desired states
 * and reconciling that with their actual states.</p>
 *
 * <p>Where the desired states come from depends on the PKI concerned.
 * For the cluster CA they're supplied by the CO (based on the certificates issued by the CertificateController).
 * For the clients CA we need some shared state, such as a Secret:</p>
 * <pre><code>
 * kind: Secret
 *   metadata:
 *   # the input secret
 *   name: my-cluster-clients-ca-input
 * data:
 *   fingerprint.state: ... # what the current state should be (what happens if this is an illegal state transition??)
 * </code></pre>
 * <p>But that's done outside this class.</p>
 */
public class TrustController {

    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(TrustController.class);

    private final Reconciliation reconciliation;
    private final TrustPrivateState privateState;
    private final List<TrustOutputWriter> outputWriters;

    public TrustController(Reconciliation reconciliation, TrustPrivateState privateState, List<TrustOutputWriter> outputWriters) {
        this.reconciliation = reconciliation;
        this.privateState = privateState;
        this.outputWriters = outputWriters;
    }

    /**
     * Reconcile the current state with the desired state
     * @param desired The desired state
     * @return true if there were any differences
     * @throws CertificateEncodingException If something goes wrong.
     */
    public boolean reconcile(Set<TrustRoot> desired) throws CertificateEncodingException {
        // Load from private state.
        Map<Fingerprint, TrustRoot> current = privateState.load();

        // reconcile the trust input with the private state making transitions in-memory.
        Map<Fingerprint, TrustRoot> desiredMap = desired.stream().collect(Collectors.toMap(TrustRoot::fingerprint, Function.identity()));
        Map<Fingerprint, TrustRoot> updatedRoots = new HashMap<>(desiredMap.size());
        for (var entry : current.entrySet()) {
            var f = entry.getKey();
            TrustRoot currentRoot = entry.getValue();
            final TrustRoot updatedRoot;
            if (desiredMap.containsKey(f)) {
                // Do their states differ?
                TrustRoot desiredRoot = desiredMap.get(f);
                if (currentRoot.state() != desiredRoot.state()) {
                    updatedRoot = transition(currentRoot, desiredRoot);
                } else {
                    updatedRoot = currentRoot;
                }
                updatedRoots.put(updatedRoot.fingerprint(), updatedRoot);
            } else {
                // Deleted (done later)
                updatedRoot = null;
            }
            if (updatedRoot != null) {
                updatedRoots.put(updatedRoot.fingerprint(), updatedRoot);
            }
        }
        for (var entry : desiredMap.entrySet()) {
            if (!current.containsKey(entry.getKey())) {
                // New
                TrustRoot newRoot = added(entry.getValue());
                updatedRoots.put(newRoot.fingerprint(), newRoot);
            }
        }

        // Persist the private state
        privateState.transition(updatedRoots);

        Map<Fingerprint, TrustRoot> current2 = privateState.load();
        Map<Fingerprint, TrustRoot> toRemove = new HashMap<>(desiredMap.size());
        for (var entry : current2.entrySet()) {
            var f = entry.getKey();
            TrustRoot currentRoot = entry.getValue();
            if (!desiredMap.containsKey(f)) {
                // Deleted
                Fingerprint deleted = deleted(currentRoot);
                toRemove.put(deleted, currentRoot);
            }
        }
        privateState.remove(toRemove);

        // Update the trust outputs
        for (TrustOutputWriter writer : outputWriters) {
            writer.write(updatedRoots.values());
        }
        return !updatedRoots.equals(current) || !toRemove.isEmpty();
    }

    private TrustRoot added(TrustRoot root) {
        LOGGER.debugCr(reconciliation, "Adding {} in state {}", root.fingerprint(), root.state());
        return root;
    }

    private Fingerprint deleted(TrustRoot root) {
        LOGGER.debugCr(reconciliation, "Removing {} in state {}", root.fingerprint(), root.state());
        return root.fingerprint();
    }

    private TrustRoot transition(TrustRoot currentRoot, TrustRoot desiredRoot) {
        TrustState desiredState = desiredRoot.state();
        TrustState currentState = currentRoot.state();
        if (currentState.equals(desiredState)) {
            throw new RuntimeException("Assertion failed");
        }
        switch (currentState) {
            case UNKNOWN:
                if (desiredState != TrustState.UNTRUSTED) {
                    throw new RuntimeException("Illegal state transition: " + currentState + " -> " + desiredRoot);
                }
                break;
            case UNTRUSTED:
                if (desiredState != TrustState.TRUSTED
                    && desiredState != TrustState.ZOMBIE) {
                    throw new RuntimeException("Illegal state transition: " + currentState + " -> " + desiredRoot);
                }
                break;
            case TRUSTED:
                if (desiredState != TrustState.PHASE_OUT) {
                    throw new RuntimeException("Illegal state transition: " + currentState + " -> " + desiredRoot);
                }
                break;
            case PHASE_OUT:
                if (desiredState != TrustState.ZOMBIE) {
                    throw new RuntimeException("Illegal state transition: " + currentState + " -> " + desiredRoot);
                }
                break;

        }
        LOGGER.debugCr(reconciliation, "Transitioning {} from {} to {}", currentRoot.fingerprint(), currentState, desiredState);
        return new TrustRoot(desiredRoot.x509(), desiredState, currentRoot.resourceVersion());
    }
}
