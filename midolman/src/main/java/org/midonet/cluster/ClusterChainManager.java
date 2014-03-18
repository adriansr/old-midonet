/*
 * Copyright 2012 Midokura Europe SARL
 */

package org.midonet.cluster;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.midonet.midolman.rules.Rule;
import org.midonet.midolman.state.zkManagers.RuleZkManager;
import org.midonet.cluster.client.ChainBuilder;

public class ClusterChainManager extends ClusterManager<ChainBuilder> {
    private static final Logger log = LoggerFactory
        .getLogger(ClusterChainManager.class);

    @Inject
    RuleZkManager ruleMgr;

    private Map<UUID, Map<UUID, Rule>> chainIdToRuleMap =
            new HashMap<UUID, Map<UUID, Rule>>();
    private Map<UUID, List<UUID>> chainToRuleIds =
            new HashMap<UUID, List<UUID>>();
    private Multimap<UUID, UUID> chainToMissingRuleIds =
            HashMultimap.create();

    @Override
    protected void getConfig(UUID chainId) {
        if (chainIdToRuleMap.containsKey(chainId)) {
            log.error("Trying to request the same Chain {}.", chainId);
            return;
        }
        chainIdToRuleMap.put(chainId, new HashMap<UUID, Rule>());
        RuleListCallback ruleListCB = new RuleListCallback(chainId);
        ruleMgr.getRuleIdListAsync(chainId, ruleListCB, ruleListCB);
    }

    private void requestRule(UUID ruleID) {
        RuleCallback ruleCallback = new RuleCallback(ruleID);
        ruleMgr.getAsync(ruleID, ruleCallback, ruleCallback);
    }

    private class RuleListCallback extends CallbackWithWatcher<List<UUID>> {
        private UUID chainId;

        private RuleListCallback(UUID chainId) {
            this.chainId = chainId;
        }

        @Override
        protected String describe() {
            return "RuleList:" + chainId;
        }

        @Override
        public void onSuccess(Result<List<UUID>> data) {
            // This is an ordered list of the UUIDs of current rules
            List<UUID> curRuleIds = data.getData();

            // UUID to actual rule for each rule in chain
            Map<UUID, Rule> ruleMap = chainIdToRuleMap.get(chainId);

            // If null, we no longer care about this chainId.
            if (null == ruleMap) {
                chainToRuleIds.remove(chainId);
                return;
            } else {
                chainToRuleIds.put(chainId, curRuleIds);
            }

            // List of old rule IDs from chain
            Set<UUID> oldRuleIds = ruleMap.keySet();

            // If the new ordered list tells us a rule disappeared,
            // remove it from the chain's rule id -> rule info map
            Iterator<UUID> ruleIter = oldRuleIds.iterator();
            while (ruleIter.hasNext()) {
                if (!curRuleIds.contains(ruleIter.next()))
                    ruleIter.remove();
            }

            // If we have all the rules in the new ordered list, we're
            // ready to call the chainbuilder
            if (oldRuleIds.size() == curRuleIds.size()) {
                getBuilder(chainId).setRules(curRuleIds, ruleMap);
                return;
            }
            // Otherwise, we have to fetch some rules.
            for (UUID ruleId : curRuleIds) {
                if (!oldRuleIds.contains(ruleId)) {
                    chainToMissingRuleIds.put(chainId, ruleId);
                    requestRule(ruleId);
                }
            }
        }

        @Override
        public void pathDataChanged(String path) {
            ruleMgr.getRuleIdListAsync(chainId, this, this);
        }

        @Override
        protected Runnable makeRetry() {
            return new Runnable() {
                @Override
                public void run() {
                    ruleMgr.getRuleIdListAsync(chainId,
                            RuleListCallback.this, RuleListCallback.this);
                }
            };
        }

    }

    private class RuleCallback extends CallbackWithWatcher<Rule> {
        private UUID ruleId;

        private RuleCallback(UUID ruleId) {
            this.ruleId = ruleId;
        }

        @Override
        protected String describe() {
            return "Rule:" + ruleId;
        }

        @Override
        public void onSuccess(Result<Rule> data) {
            Rule rule = data.getData();
            Collection<UUID> missingRuleIds =
                    chainToMissingRuleIds.get(rule.chainId);
            List<UUID> ruleIds = chainToRuleIds.get(rule.chainId);
            // Does the chain still care about this rule?
            if (ruleIds == null || ! ruleIds.contains(ruleId))
                return;
            missingRuleIds.remove(ruleId);
            Map<UUID, Rule> ruleMap = chainIdToRuleMap.get(rule.chainId);

            ruleMap.put(ruleId, rule);

            if ((missingRuleIds.isEmpty())) {
                getBuilder(rule.chainId).setRules(ruleIds, ruleMap);
            }
        }

        @Override
        public void pathDataChanged(String path) {
            ruleMgr.getAsync(ruleId, this, this);
        }

        @Override
        protected Runnable makeRetry() {
            return new Runnable() {
                @Override
                public void run() {
                    ruleMgr.getAsync(ruleId,
                        RuleCallback.this, RuleCallback.this);
                }
            };
        }
    }
}
