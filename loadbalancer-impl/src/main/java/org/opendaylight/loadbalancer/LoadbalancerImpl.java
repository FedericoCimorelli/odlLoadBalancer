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
import org.opendaylight.yang.gen.v1.http.netconfcentral.org.ns.loadbalancer.rev150901.Loadbalancer;
import org.opendaylight.yang.gen.v1.http.netconfcentral.org.ns.loadbalancer.rev150901.Loadbalancer.LoadbalancerStatus;
import org.opendaylight.yang.gen.v1.http.netconfcentral.org.ns.loadbalancer.rev150901.LoadbalancerBuilder;
import org.opendaylight.yang.gen.v1.http.netconfcentral.org.ns.loadbalancer.rev150901.LoadbalancerService;
import org.opendaylight.yang.gen.v1.http.netconfcentral.org.ns.loadbalancer.rev150901.StartLoadbalancerInput;
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

// Yangtools methods to manipulate RPC DTOs
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
    public void onDataChanged(
            final AsyncDataChangeEvent<InstanceIdentifier<?>, DataObject> change) {
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




    @Override
    public Future<RpcResult<Void>> startLoadbalancer(StartLoadbalancerInput input) {
        LOG.info("Test start loadbalancer service - start");
        // TESTING METHODS
        testAccessDataInMdSal();
        testSetRoleAction();
        LOG.info("Test start loadbalancer service - end");
        final SettableFuture<RpcResult<Void>> futureResult = SettableFuture.create();
        return futureResult;
    }






    private void testSetRoleAction() {
        //RoleRequestOutput roleRequestOutput;
//        RoleRequestInput roleRequestInput;
//        final BigInteger generationId = BigInteger.TEN;

//        roleRequestOutput = new RoleRequestOutputBuilder()
//            .setGenerationId(generationId)
//            .setRole(ControllerRole.OFPCRROLESLAVE)
//            .setVersion((short)42)
//            .setXid(21L)
//            .build();
//
//        ControllerRole ofJavaRole = RoleUtil.toOFJavaRole(OfpRole.BECOMEMASTER);
//        roleRequestInput = new RoleRequestInputBuilder()
//                .setGenerationId(generationId)
//                .setRole(ofJavaRole)
//                .setVersion((short)42)
//                .setXid(21L)
//                .build();


        /**
         * no change to role
         *NOCHANGE(0),
         * promote current role to MASTER
         *BECOMEMASTER(1)
         * demote current role to SLAVE
         *BECOMESLAVE(2)
         */

//        LOG.info("Loadbalancer, RoleManager - start");
//        OFRoleManager roleManager = null;
//        roleManager = new OFRoleManager(OFSessionUtil.getSessionManager());
//        LOG.info("Loadbalancer, RoleManager - BECOMEMASTER");
//        roleManager.manageRoleChange(1);
//        LOG.info("Loadbalancer, RoleManager - BECOMESLAVE");
//        roleManager.manageRoleChange(2);
//        LOG.info("Loadbalancer, RoleManager - end");
//        try {
//            roleManager.close();
//        } catch (Exception e) {
//            String se = "Loadbalancer, RoleManager - "+e.getMessage();
//            LOG.error(se);
//        }
    }





    private void initLoadbalancerOperational() {
        Loadbalancer loadbalancer = new LoadbalancerBuilder()
                .setDarknessFactor(1000l)
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
        LOG.info(
                "initLoadbalancerOperational: operational status populated: {}",
                loadbalancer);
    }

    private void initLoadbalancerConfiguration() {
        Loadbalancer toaster = new LoadbalancerBuilder().setDarknessFactor(
                (long) 1000).build();
        WriteTransaction tx = dataBroker.newWriteOnlyTransaction();
        tx.put(LogicalDatastoreType.CONFIGURATION, LOADBALANCER_IID, toaster);
        tx.submit();
        LOG.info("initLoadbalancerConfiguration: default config populated: {}",
                toaster);
    }

}


//package org.opendaylight.loadbalancer;
//
//import com.google.common.base.Optional;
//import com.google.common.util.concurrent.FutureCallback;
//import com.google.common.util.concurrent.Futures;
//import com.google.common.util.concurrent.SettableFuture;
//
//import java.util.List;
//import java.util.concurrent.Future;
//
//import org.opendaylight.controller.md.sal.binding.api.DataBroker;
//import org.opendaylight.controller.md.sal.binding.api.DataChangeListener;
//import org.opendaylight.controller.md.sal.binding.api.ReadOnlyTransaction;
//import org.opendaylight.controller.md.sal.binding.api.WriteTransaction;
//import org.opendaylight.controller.md.sal.common.api.data.AsyncDataBroker.DataChangeScope;
//import org.opendaylight.controller.md.sal.common.api.data.AsyncDataChangeEvent;
//import org.opendaylight.controller.md.sal.common.api.data.LogicalDatastoreType;
//import org.opendaylight.controller.sal.binding.api.BindingAwareBroker;
//import org.opendaylight.controller.sal.binding.api.BindingAwareBroker.ProviderContext;
//import org.opendaylight.controller.sal.binding.api.BindingAwareProvider;
//import org.opendaylight.yang.gen.v1.http.netconfcentral.org.ns.loadbalancer.rev150901.Loadbalancer;
//import org.opendaylight.yang.gen.v1.http.netconfcentral.org.ns.loadbalancer.rev150901.Loadbalancer.LoadbalancerStatus;
//import org.opendaylight.yang.gen.v1.http.netconfcentral.org.ns.loadbalancer.rev150901.LoadbalancerBuilder;
//import org.opendaylight.yang.gen.v1.http.netconfcentral.org.ns.loadbalancer.rev150901.LoadbalancerService;
//import org.opendaylight.yang.gen.v1.http.netconfcentral.org.ns.loadbalancer.rev150901.StartLoadbalancerInput;
//import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.NodeConnectorRemoved;
//import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.NodeConnectorUpdated;
//import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.NodeRemoved;
//import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.NodeUpdated;
//import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.OpendaylightInventoryListener;
//import org.opendaylight.yang.gen.v1.urn.opendaylight.packet.service.rev130709.PacketProcessingListener;
//import org.opendaylight.yang.gen.v1.urn.opendaylight.packet.service.rev130709.PacketReceived;
//import org.opendaylight.yang.gen.v1.urn.tbd.params.xml.ns.yang.network.topology.rev131021.NetworkTopology;
//import org.opendaylight.yang.gen.v1.urn.tbd.params.xml.ns.yang.network.topology.rev131021.TopologyId;
//import org.opendaylight.yang.gen.v1.urn.tbd.params.xml.ns.yang.network.topology.rev131021.network.topology.Topology;
//import org.opendaylight.yang.gen.v1.urn.tbd.params.xml.ns.yang.network.topology.rev131021.network.topology.TopologyKey;
//import org.opendaylight.yang.gen.v1.urn.tbd.params.xml.ns.yang.network.topology.rev131021.network.topology.topology.Node;
//// Yangtools methods to manipulate RPC DTOs
//import org.opendaylight.yangtools.concepts.ListenerRegistration;
//import org.opendaylight.yangtools.yang.binding.DataObject;
//import org.opendaylight.yangtools.yang.binding.InstanceIdentifier;
//import org.opendaylight.yangtools.yang.common.RpcResult;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//public class LoadbalancerImpl implements BindingAwareProvider,
//        DataChangeListener, AutoCloseable, LoadbalancerService,
//        OpendaylightInventoryListener, PacketProcessingListener {
//
//    private static final Logger LOG = LoggerFactory
//            .getLogger(LoadbalancerImpl.class);
//
//    private ProviderContext providerContext;
//    private DataBroker dataBroker;
//    private ListenerRegistration<DataChangeListener> dcReg;
//    private BindingAwareBroker.RpcRegistration<LoadbalancerService> rpcReg;
//    public static final InstanceIdentifier<Loadbalancer> LOADBALANCER_IID = InstanceIdentifier
//            .builder(Loadbalancer.class).build();
//    private static final String DEFAULT_TOPOLOGY_ID = "flow:1";
//
//    @Override
//    public void close() throws Exception {
//        dcReg.close();
//        rpcReg.close();
//        LOG.info("Registrations closed");
//    }
//
//    @Override
//    public void onSessionInitiated(ProviderContext session) {
//        this.providerContext = session;
//        this.dataBroker = session.getSALService(DataBroker.class);
//        // Register the DataChangeListener for Loadbalancer's configuration
//        // subtree
//        dcReg = dataBroker.registerDataChangeListener(
//                LogicalDatastoreType.CONFIGURATION, LOADBALANCER_IID, this,
//                DataChangeScope.SUBTREE);
//        // Register the RPC Service
//        rpcReg = session.addRpcImplementation(LoadbalancerService.class, this);
//        // Initialize operational and default config data in MD-SAL data store
//        initLoadbalancerOperational();
//        initLoadbalancerConfiguration();
//        LOG.info("onSessionInitiated: initialization done");
//    }
//
//    @Override
//    public void onDataChanged(
//            final AsyncDataChangeEvent<InstanceIdentifier<?>, DataObject> change) {
//        DataObject dataObject = change.getUpdatedSubtree();
//        if (dataObject instanceof Loadbalancer) {
//            Loadbalancer loadbalancer = (Loadbalancer) dataObject;
//            LOG.info("onDataChanged - new Loadbalancer config: {}",
//                    loadbalancer);
//        } else {
//            LOG.warn("onDataChanged - not instance of Loadbalancer {}",
//                    dataObject);
//        }
//    }
//
//    @Override
//    public void onNodeConnectorRemoved(NodeConnectorRemoved arg0) {
//        String s = "LOADBALANCER" + arg0.toString();
//        LOG.info(s);
//    }
//
//    @Override
//    public void onNodeConnectorUpdated(NodeConnectorUpdated arg0) {
//        String s = "LOADBALANCER" + arg0.toString();
//        LOG.info(s);
//    }
//
//    @Override
//    public void onNodeRemoved(NodeRemoved arg0) {
//        String s = "LOADBALANCER" + arg0.toString();
//        LOG.info(s);
//    }
//
//    @Override
//    public void onNodeUpdated(NodeUpdated arg0) {
//        String s = "LOADBALANCER" + arg0.toString();
//        LOG.info(s);
//    }
//
//    @Override
//    public void onPacketReceived(PacketReceived arg0) {
//        String s = "LOADBALANCER" + arg0.toString();
//        LOG.info(s);
//    }
//
//    private void testAccessDataInMdSal() {
//        LOG.info("Test access data in MD-SAL - start");
//        // Get topology and extract links
//        InstanceIdentifier<Topology> topologyInstanceIdentifier = InstanceIdentifier
//                .builder(NetworkTopology.class)
//                .child(Topology.class,
//                        new TopologyKey(new TopologyId(DEFAULT_TOPOLOGY_ID)))
//                .build();
//        Topology topology = null;
//        ReadOnlyTransaction readOnlyTransaction = dataBroker
//                .newReadOnlyTransaction();
//        try {
//            Optional<Topology> topologyOptional = readOnlyTransaction.read(
//                    LogicalDatastoreType.OPERATIONAL,
//                    topologyInstanceIdentifier).get();
//            if (topologyOptional.isPresent()) {
//                topology = topologyOptional.get();
//                LOG.info("Test access data in MD-SAL - topology is present");
//            }
//        } catch (Exception e) {
//            LOG.error("Error reading topology {}", topologyInstanceIdentifier);
//            readOnlyTransaction.close();
//            throw new RuntimeException("Error reading from operational store, topology : " + topologyInstanceIdentifier, e);
//        }
//        readOnlyTransaction.close();
//        if (topology == null) {
//            LOG.info("Test access data in MD-SAL - topology is null");
//        }
//
//        List<Node> nodes = topology.getNode();
//        if(nodes != null){
//            LOG.info("Test access data in MD-SAL - nodes are present");
//            LOG.info(nodes.toString());
//            String s = "WWWWWWWWWWWWW "+nodes.size();
//            LOG.info(s);
//            Node n = nodes.get(0);
//            if(n != null){
//                LOG.info("1111111111111111");
//                LOG.info("Test access data in MD-SAL - node at index 0 is present");
//                LOG.info(n.toString());
//                LOG.info("2222222222222222");
//            }
//            else{
//                LOG.info("Test access data in MD-SAL - node at index 0 is null");
//            }
//        }
//        else{
//            LOG.info("Test access data in MD-SAL - nodes is null");
//        }
//        LOG.info("Test access data in MD-SAL - end");
//    }
//
//    @Override
//    public Future<RpcResult<Void>> startLoadbalancer(
//            StartLoadbalancerInput input) {
//        LOG.info("Test start loadbalancer service - start");
//        // TESTING METHODS
//        testAccessDataInMdSal();
//        LOG.info("Test start loadbalancer service - end");
//        final SettableFuture<RpcResult<Void>> futureResult = SettableFuture
//                .create();
//        return futureResult;
//    }
//
//    private void initLoadbalancerOperational() {
//        Loadbalancer loadbalancer = new LoadbalancerBuilder()
//                .setDarknessFactor(1000l)
//                .setLoadbalancerStatus(LoadbalancerStatus.Down).build();
//        WriteTransaction tx = dataBroker.newWriteOnlyTransaction();
//        tx.put(LogicalDatastoreType.OPERATIONAL, LOADBALANCER_IID, loadbalancer);
//        Futures.addCallback(tx.submit(), new FutureCallback<Void>() {
//            @Override
//            public void onSuccess(final Void result) {
//                LOG.info("initLoadbalancerOperational: transaction succeeded");
//            }
//
//            @Override
//            public void onFailure(final Throwable t) {
//                LOG.error("initLoadbalancerOperational: transaction failed");
//            }
//        });
//        LOG.info(
//                "initLoadbalancerOperational: operational status populated: {}",
//                loadbalancer);
//    }
//
//    private void initLoadbalancerConfiguration() {
//        Loadbalancer toaster = new LoadbalancerBuilder().setDarknessFactor(
//                (long) 1000).build();
//        WriteTransaction tx = dataBroker.newWriteOnlyTransaction();
//        tx.put(LogicalDatastoreType.CONFIGURATION, LOADBALANCER_IID, toaster);
//        tx.submit();
//        LOG.info("initLoadbalancerConfiguration: default config populated: {}",
//                toaster);
//    }
//
//}
