/*
 * Copyright (c) 2015 Sapienza University of Rome.  All rights reserved.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html
 */


package org.opendaylight.loadbalancer;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.SettableFuture;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;

import org.opendaylight.controller.md.sal.binding.api.DataBroker;
import org.opendaylight.controller.md.sal.binding.api.DataChangeListener;
import org.opendaylight.controller.md.sal.binding.api.ReadOnlyTransaction;
import org.opendaylight.controller.md.sal.binding.api.WriteTransaction;
import org.opendaylight.controller.md.sal.common.api.data.AsyncDataBroker.DataChangeScope;
import org.opendaylight.controller.md.sal.common.api.data.AsyncDataChangeEvent;
import org.opendaylight.controller.md.sal.common.api.data.LogicalDatastoreType;
import org.opendaylight.controller.sal.binding.api.BindingAwareBroker;
import org.opendaylight.controller.sal.binding.api.BindingAwareBroker.ProviderContext;
import org.opendaylight.controller.sal.binding.api.BindingAwareProvider;
import org.opendaylight.openflowplugin.api.openflow.md.core.session.SessionContext;
import org.opendaylight.openflowplugin.api.openflow.md.core.session.SessionManager;
import org.opendaylight.openflowplugin.openflow.md.core.session.OFSessionUtil;
import org.opendaylight.openflowplugin.openflow.md.util.RoleUtil;
import org.opendaylight.yang.gen.v1.http.netconfcentral.org.ns.loadbalancer.rev150901.GetSwitchRoleInput;
import org.opendaylight.yang.gen.v1.http.netconfcentral.org.ns.loadbalancer.rev150901.GetSwitchRoleOutput;
import org.opendaylight.yang.gen.v1.http.netconfcentral.org.ns.loadbalancer.rev150901.Loadbalancer;
import org.opendaylight.yang.gen.v1.http.netconfcentral.org.ns.loadbalancer.rev150901.Loadbalancer.LoadbalancerStatus;
import org.opendaylight.yang.gen.v1.http.netconfcentral.org.ns.loadbalancer.rev150901.LoadbalancerBuilder;
import org.opendaylight.yang.gen.v1.http.netconfcentral.org.ns.loadbalancer.rev150901.LoadbalancerService;
import org.opendaylight.yang.gen.v1.http.netconfcentral.org.ns.loadbalancer.rev150901.SetSwitchRoleInput;
import org.opendaylight.yang.gen.v1.http.netconfcentral.org.ns.loadbalancer.rev150901.SetSwitchRoleOutput;
import org.opendaylight.yang.gen.v1.http.netconfcentral.org.ns.loadbalancer.rev150901.StartLoadbalancerInput;
import org.opendaylight.yang.gen.v1.http.netconfcentral.org.ns.loadbalancer.rev150901.StartLoadbalancerOutput;
import org.opendaylight.yang.gen.v1.http.netconfcentral.org.ns.loadbalancer.rev150901.StopLoadbalancerOutput;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.NodeConnectorRemoved;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.NodeConnectorUpdated;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.NodeRemoved;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.NodeUpdated;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.OpendaylightInventoryListener;
import org.opendaylight.yang.gen.v1.urn.opendaylight.packet.service.rev130709.PacketProcessingListener;
import org.opendaylight.yang.gen.v1.urn.opendaylight.packet.service.rev130709.PacketReceived;
import org.opendaylight.yang.gen.v1.urn.tbd.params.xml.ns.yang.network.topology.rev131021.NetworkTopology;
import org.opendaylight.yang.gen.v1.urn.tbd.params.xml.ns.yang.network.topology.rev131021.TopologyId;
import org.opendaylight.yang.gen.v1.urn.tbd.params.xml.ns.yang.network.topology.rev131021.network.topology.Topology;
import org.opendaylight.yang.gen.v1.urn.tbd.params.xml.ns.yang.network.topology.rev131021.network.topology.TopologyKey;
import org.opendaylight.yang.gen.v1.urn.tbd.params.xml.ns.yang.network.topology.rev131021.network.topology.topology.Node;
import org.opendaylight.yangtools.concepts.ListenerRegistration;
import org.opendaylight.yangtools.yang.binding.DataObject;
import org.opendaylight.yangtools.yang.binding.InstanceIdentifier;
import org.opendaylight.yangtools.yang.common.RpcResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class LoadbalancerImpl implements BindingAwareProvider,
        DataChangeListener, AutoCloseable, LoadbalancerService,
        OpendaylightInventoryListener, PacketProcessingListener {

    private static final Logger LOG = LoggerFactory.getLogger(LoadbalancerImpl.class);
    private ProviderContext providerContext;
    private DataBroker dataBroker;
    private ListenerRegistration<DataChangeListener> dcReg;
    private BindingAwareBroker.RpcRegistration<LoadbalancerService> rpcReg;
    public static final InstanceIdentifier<Loadbalancer> LOADBALANCER_IID = InstanceIdentifier.builder(Loadbalancer.class).build();
    private static final String DEFAULT_TOPOLOGY_ID = "flow:1";



    @Override
    public void close() throws Exception {
        dcReg.close();
        rpcReg.close();
        LOG.info("Registrations closed");
    }



    @Override
    public void onSessionInitiated(ProviderContext session) {
        this.providerContext = session;
        this.dataBroker = session.getSALService(DataBroker.class);
        dcReg = dataBroker.registerDataChangeListener(
                LogicalDatastoreType.CONFIGURATION, LOADBALANCER_IID, this,
                DataChangeScope.SUBTREE);
        rpcReg = session.addRpcImplementation(LoadbalancerService.class, this);
        initLoadbalancerOperational();
        initLoadbalancerConfiguration();
        LOG.info("onSessionInitiated: initialization done");
    }




    @Override
    public void onDataChanged(final AsyncDataChangeEvent<InstanceIdentifier<?>, DataObject> change) {
        DataObject dataObject = change.getUpdatedSubtree();
        if (dataObject instanceof Loadbalancer) {
            Loadbalancer loadbalancer = (Loadbalancer) dataObject;
            LOG.info("onDataChanged - new Loadbalancer config: {}",
                    loadbalancer);
        } else {
            LOG.warn("onDataChanged - not instance of Loadbalancer {}",
                    dataObject);
        }
    }



    @Override
    public void onNodeConnectorRemoved(NodeConnectorRemoved arg0) {
        String s = "LOADBALANCER" + arg0.toString();
        LOG.info(s);
    }



    @Override
    public void onNodeConnectorUpdated(NodeConnectorUpdated arg0) {
        String s = "LOADBALANCER" + arg0.toString();
        LOG.info(s);
    }



    @Override
    public void onNodeRemoved(NodeRemoved arg0) {
        String s = "LOADBALANCER" + arg0.toString();
        LOG.info(s);
    }



    @Override
    public void onNodeUpdated(NodeUpdated arg0) {
        String s = "LOADBALANCER" + arg0.toString();
        LOG.info(s);
    }



    @Override
    public void onPacketReceived(PacketReceived arg0) {
        String s = "LOADBALANCER" + arg0.toString();
        LOG.info(s);
    }



    private void testAccessDataInMdSal() {
        LOG.info("Test access data in MD-SAL - start");
        InstanceIdentifier<Topology> topologyInstanceIdentifier = InstanceIdentifier.builder(
                NetworkTopology.class).child(
                        Topology.class, new TopologyKey(new TopologyId(DEFAULT_TOPOLOGY_ID))).build();
        Topology topology = null;
        ReadOnlyTransaction readOnlyTransaction = dataBroker.newReadOnlyTransaction();
        try {
            Optional<Topology> topologyOptional = readOnlyTransaction.read(
                    LogicalDatastoreType.OPERATIONAL,
                    topologyInstanceIdentifier).get();
            if (topologyOptional.isPresent()) {
                topology = topologyOptional.get();
                LOG.info("Test access data in MD-SAL - topology is present");
            }
        } catch (Exception e) {
            LOG.error("Error reading topology {}", topologyInstanceIdentifier);
            readOnlyTransaction.close();
            throw new RuntimeException("Error reading from operational store, topology : " + topologyInstanceIdentifier, e);
        }
        readOnlyTransaction.close();
        if (topology == null) {
            LOG.info("Test access data in MD-SAL - topology is null");
        }

        List<Node> nodes = topology.getNode();
        for(Node n : nodes){
            LOG.info(n.toString());
        }
        LOG.info("Test access data in MD-SAL - end");
    }




    private void initLoadbalancerOperational() {
        Loadbalancer loadbalancer = new LoadbalancerBuilder()
                //.setDarknessFactor(1000l)
                .setLoadbalancerStatus(LoadbalancerStatus.Down).build();
        WriteTransaction tx = dataBroker.newWriteOnlyTransaction();
        tx.put(LogicalDatastoreType.OPERATIONAL, LOADBALANCER_IID, loadbalancer);
        Futures.addCallback(tx.submit(), new FutureCallback<Void>() {
            @Override
            public void onSuccess(final Void result) {
                LOG.info("initLoadbalancerOperational: transaction succeeded");
            }
            @Override
            public void onFailure(final Throwable t) {
                LOG.error("initLoadbalancerOperational: transaction failed");
            }
        });
        LOG.info("initLoadbalancerOperational: operational status populated: {}", loadbalancer);
    }



    private void initLoadbalancerConfiguration() {
        Loadbalancer loadbalancer = new LoadbalancerBuilder()/*.setDarknessFactor((long) 1000)*/.build();
        WriteTransaction tx = dataBroker.newWriteOnlyTransaction();
        tx.put(LogicalDatastoreType.CONFIGURATION, LOADBALANCER_IID, loadbalancer);
        tx.submit();
        LOG.info("initLoadbalancerConfiguration: default config populated: {}", loadbalancer);
    }



    private void testSetRoleAction() {
        LOG.info("Loadbalancer, RoleManager - start");
        LOG.info("Loadbalancer, RoleManager - test BECOMESLAVE");
        List<String> dpids = new ArrayList<String>();
        dpids.add("1");
        RoleUtil.fireRoleChange(2, dpids);
        /**
         * no change to role
         *NOCHANGE(0),
         * promote current role to MASTER
         *BECOMEMASTER(1)
         * demote current role to SLAVE
         *BECOMESLAVE(2)
         */

        SessionManager sessionManager = OFSessionUtil.getSessionManager();
        for (final SessionContext session : sessionManager.getAllSessions()) {
            LOG.warn("WWWWWWWW", session.getRoleOnDevice().toString());
            LOG.warn("WWWWWWWW", session.getRoleOnDevice().getIntValue());
        }
    }





    @Override
    public Future<RpcResult<StartLoadbalancerOutput>> startLoadbalancer(StartLoadbalancerInput input) {
        LOG.info("Test start loadbalancer service - start");
        // TESTING METHODS
        testAccessDataInMdSal();
        LOG.info("Test start loadbalancer service - end");
        final SettableFuture<RpcResult<StartLoadbalancerOutput>> futureResult = SettableFuture.create();
        //futureResult.set(1);
        return futureResult;
    }



    @Override
    public Future<RpcResult<StopLoadbalancerOutput>> stopLoadbalancer() {
        // TODO Auto-generated method stub
        return null;
    }



    @Override
    public Future<RpcResult<SetSwitchRoleOutput>> setSwitchRole(
            SetSwitchRoleInput input) {
        // TODO Auto-generated method stub
        return null;
    }



    @Override
    public Future<RpcResult<GetSwitchRoleOutput>> getSwitchRole(GetSwitchRoleInput input) {
        // TODO Auto-generated method stub
        return null;
    }




//    @Override
//    public Future<RpcResult<Void>> setSwitchRole(SetSwitchRoleInput input) {
//        LOG.info("LB_SATDATAPATH_ROLE_1: :", input.getOfpRole());
//        //LOG.info("LB_SATDATAPATH_ROLE_2: :", input.getDatapathIds().get(0).toString());
//        //LOG.info("LB_SATDATAPATH_ROLE_3: :", input.getDatapathIds().toArray().toString());
//        testSetRoleAction();
//        return null;
//    }




}
