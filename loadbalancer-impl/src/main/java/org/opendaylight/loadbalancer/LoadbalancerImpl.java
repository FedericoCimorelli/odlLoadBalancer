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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
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
import org.opendaylight.openflowplugin.openflow.md.util.RoleUtil;
import org.opendaylight.yang.gen.v1.http.netconfcentral.org.ns.loadbalancer.rev150901.GetLoadbalancerStatusOutput;
import org.opendaylight.yang.gen.v1.http.netconfcentral.org.ns.loadbalancer.rev150901.GetLoadbalancerStatusOutputBuilder;
import org.opendaylight.yang.gen.v1.http.netconfcentral.org.ns.loadbalancer.rev150901.GetSwitchRoleInput;
import org.opendaylight.yang.gen.v1.http.netconfcentral.org.ns.loadbalancer.rev150901.GetSwitchRoleOutput;
import org.opendaylight.yang.gen.v1.http.netconfcentral.org.ns.loadbalancer.rev150901.GetSwitchRoleOutputBuilder;
import org.opendaylight.yang.gen.v1.http.netconfcentral.org.ns.loadbalancer.rev150901.Loadbalancer;
import org.opendaylight.yang.gen.v1.http.netconfcentral.org.ns.loadbalancer.rev150901.Loadbalancer.LoadbalancerStatus;
import org.opendaylight.yang.gen.v1.http.netconfcentral.org.ns.loadbalancer.rev150901.LoadbalancerBuilder;
import org.opendaylight.yang.gen.v1.http.netconfcentral.org.ns.loadbalancer.rev150901.LoadbalancerService;
import org.opendaylight.yang.gen.v1.http.netconfcentral.org.ns.loadbalancer.rev150901.SetSwitchRoleInput;
import org.opendaylight.yang.gen.v1.http.netconfcentral.org.ns.loadbalancer.rev150901.SetSwitchRoleOutput;
import org.opendaylight.yang.gen.v1.http.netconfcentral.org.ns.loadbalancer.rev150901.SetSwitchRoleOutputBuilder;
import org.opendaylight.yang.gen.v1.http.netconfcentral.org.ns.loadbalancer.rev150901.StartLoadbalancerInput;
import org.opendaylight.yang.gen.v1.http.netconfcentral.org.ns.loadbalancer.rev150901.StartLoadbalancerOutput;
import org.opendaylight.yang.gen.v1.http.netconfcentral.org.ns.loadbalancer.rev150901.StartLoadbalancerOutputBuilder;
import org.opendaylight.yang.gen.v1.http.netconfcentral.org.ns.loadbalancer.rev150901.StopLoadbalancerOutput;
import org.opendaylight.yang.gen.v1.http.netconfcentral.org.ns.loadbalancer.rev150901.StopLoadbalancerOutputBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.NodeConnectorRemoved;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.NodeConnectorUpdated;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.NodeRemoved;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.NodeUpdated;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.OpendaylightInventoryListener;
import org.opendaylight.yang.gen.v1.urn.opendaylight.packet.service.rev130709.PacketProcessingListener;
import org.opendaylight.yang.gen.v1.urn.opendaylight.packet.service.rev130709.PacketReceived;
import org.opendaylight.yangtools.concepts.ListenerRegistration;
import org.opendaylight.yangtools.yang.binding.DataObject;
import org.opendaylight.yangtools.yang.binding.InstanceIdentifier;
import org.opendaylight.yangtools.yang.common.RpcResult;
import org.opendaylight.yangtools.yang.common.RpcResultBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class LoadbalancerImpl implements BindingAwareProvider,
        DataChangeListener, AutoCloseable, LoadbalancerService,
        OpendaylightInventoryListener, PacketProcessingListener {

    private static final Logger LOG = LoggerFactory.getLogger(LoadbalancerImpl.class);
    private static String TAG = "LoadBalancer";
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
        LOG.info(TAG, "Registrations closed");
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
        LOG.info(TAG, "onSessionInitiated: initialization done");
    }




    @Override
    public void onDataChanged(final AsyncDataChangeEvent<InstanceIdentifier<?>, DataObject> change) {
        DataObject dataObject = change.getUpdatedSubtree();
        if (dataObject instanceof Loadbalancer) {
            Loadbalancer loadbalancer = (Loadbalancer) dataObject;
            LOG.info(TAG, "onDataChanged - new Loadbalancer config: {}",
                    loadbalancer);
        } else {
            LOG.warn(TAG, "onDataChanged - not instance of Loadbalancer {}",
                    dataObject);
        }
    }



    @Override
    public void onNodeConnectorRemoved(NodeConnectorRemoved arg0) {
        String s = "LOADBALANCER" + arg0.toString();
        LOG.info(TAG, s);
    }



    @Override
    public void onNodeConnectorUpdated(NodeConnectorUpdated arg0) {
        String s = "LOADBALANCER" + arg0.toString();
        LOG.info(TAG, s);
    }



    @Override
    public void onNodeRemoved(NodeRemoved arg0) {
        String s = "LOADBALANCER" + arg0.toString();
        LOG.info(TAG, s);
    }



    @Override
    public void onNodeUpdated(NodeUpdated arg0) {
        String s = "LOADBALANCER" + arg0.toString();
        LOG.info(TAG, s);
    }



    @Override
    public void onPacketReceived(PacketReceived arg0) {
        String s = "LOADBALANCER" + arg0.toString();
        LOG.info(TAG, s);
    }



/*    private void testAccessDataInMdSal() {
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
    }*/




    private void initLoadbalancerOperational() {
        Loadbalancer loadbalancer = new LoadbalancerBuilder().setLoadbalancerStatus(LoadbalancerStatus.Down).build();
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






    @Override
    public Future<RpcResult<StartLoadbalancerOutput>> startLoadbalancer(StartLoadbalancerInput input) {
        LOG.info(TAG, "Starting LoadBalancer...");
        LOG.info(TAG, "Write loadbalancer status in datastore");
        Loadbalancer loadbalancer = new LoadbalancerBuilder().setLoadbalancerStatus(LoadbalancerStatus.Up).build();
        WriteTransaction tx = dataBroker.newWriteOnlyTransaction();
        tx.put(LogicalDatastoreType.OPERATIONAL, LOADBALANCER_IID, loadbalancer);
        Futures.addCallback(tx.submit(), new FutureCallback<Void>() {
            @Override
            public void onSuccess(final Void result) {
            }
            @Override
            public void onFailure(final Throwable t) {
            }
        });
        //
        //TODO
        //
        //
        //
        StartLoadbalancerOutputBuilder slbob = new StartLoadbalancerOutputBuilder();
        slbob.setResponseCode(1L);
        slbob.setResponseMessage("LoadBalancer started");
        LOG.info(TAG, "LoadBalancer started!");
        return RpcResultBuilder.success(slbob.build()).buildFuture();
    }





    @Override
    public Future<RpcResult<GetLoadbalancerStatusOutput>> getLoadbalancerStatus() {
        LOG.info(TAG, "Get LoadBalancer status started...");
        LOG.info(TAG, "Reading LoadBalancer status");
        ReadOnlyTransaction tx = dataBroker.newReadOnlyTransaction();
        Optional<Loadbalancer> loadbalancer = null;
        GetLoadbalancerStatusOutputBuilder glbsob = new GetLoadbalancerStatusOutputBuilder();
        try {
            loadbalancer = tx.read(LogicalDatastoreType.OPERATIONAL, LOADBALANCER_IID).get();
        } catch (InterruptedException | ExecutionException e) {
            LOG.error(TAG, "Error when retrieving the LoadBalancer status");
            glbsob.setResponseCode(-1L);
        }
        if(loadbalancer!=null && loadbalancer.isPresent()){
            LOG.error(TAG, "LoadBalancer status null or not present");
            long status = loadbalancer.get().getLoadbalancerStatus().getIntValue();
            glbsob.setResponseCode(status);
        }
        else
            glbsob.setResponseCode(-1L);
        LOG.info(TAG, "Returing LoadBalancer status");
        return RpcResultBuilder.success(glbsob.build()).buildFuture();
    }






    @Override
    public Future<RpcResult<StopLoadbalancerOutput>> stopLoadbalancer() {
        LOG.info(TAG, "Stopping LoadBalancer...");
        Loadbalancer loadbalancer = new LoadbalancerBuilder().setLoadbalancerStatus(LoadbalancerStatus.Down).build();
        WriteTransaction tx = dataBroker.newWriteOnlyTransaction();
        tx.put(LogicalDatastoreType.OPERATIONAL, LOADBALANCER_IID, loadbalancer);
        Futures.addCallback(tx.submit(), new FutureCallback<Void>() {
            @Override
            public void onSuccess(final Void result) {
            }
            @Override
            public void onFailure(final Throwable t) {
            }
        });
        //
        //TODO
        //
        //
        //
        StopLoadbalancerOutputBuilder slbob = new StopLoadbalancerOutputBuilder();
        slbob.setResponseCode(1L);
        LOG.info(TAG, "LoadBalancer stopped!");
        return RpcResultBuilder.success(slbob.build()).buildFuture();
    }




    @Override
    public Future<RpcResult<SetSwitchRoleOutput>> setSwitchRole(SetSwitchRoleInput input) {
        LOG.info(TAG, "Set switches role stated");
        //
        //TODO input validation
        //
        //
        int role = 0;
        try{
            role = Integer.parseInt(input.getOfpRole()+"");
        }catch(RuntimeException e){
            LOG.error(TAG, "Error while parsing the request's role");
            SetSwitchRoleOutputBuilder swrob = new SetSwitchRoleOutputBuilder();
            swrob.setResponseCode(-1L);
            swrob.setResponseMessage("Error while parsing the request's role");
            return RpcResultBuilder.success(swrob.build()).buildFuture();
        }
        /**
         * no change to role
         * NOCHANGE(0),
         * promote current role to MASTER
         * BECOMEMASTER(1)
         * demote current role to SLAVE
         * BECOMESLAVE(2)
         */
        if(role !=0 && input.getSwitchIds().size()>0){
            RoleUtil.fireRoleChange(role, input.getSwitchIds());
            SetSwitchRoleOutputBuilder swrob = new SetSwitchRoleOutputBuilder();
            swrob.setResponseCode(0L);
            swrob.setResponseMessage("Switch(es) role changed");
            return RpcResultBuilder.success(swrob.build()).buildFuture();
        }
        else{
            if(role==0){
                LOG.warn(TAG, "Requested role change is 0 (NOCHANGE role), nothing to do...");
                SetSwitchRoleOutputBuilder swrob = new SetSwitchRoleOutputBuilder();
                swrob.setResponseCode(0L);
                swrob.setResponseMessage("OK");
                return RpcResultBuilder.success(swrob.build()).buildFuture();
            }
            else{
                LOG.warn(TAG, "Requested role change is empty dpIDs list, nothing to do...");
                SetSwitchRoleOutputBuilder swrob = new SetSwitchRoleOutputBuilder();
                swrob.setResponseCode(0L);
                swrob.setResponseMessage("OK");
                return RpcResultBuilder.success(swrob.build()).buildFuture();
            }
        }
    }



    @Override
    public Future<RpcResult<GetSwitchRoleOutput>> getSwitchRole(GetSwitchRoleInput input) {
        LOG.info(TAG, "Getting switches roles started");
        List<String> dpRoles = new ArrayList<String>();
        Map<String, String> swsRoles = RoleUtil.getSwitchesRoles();
        if(swsRoles==null){
            LOG.error(TAG, "Error while retieving the switches roles");
            GetSwitchRoleOutputBuilder gsrob = new GetSwitchRoleOutputBuilder();
            gsrob.setResponseCode(-1L);
            gsrob.setResponseMessage(new ArrayList<String>());
            return RpcResultBuilder.success(gsrob.build()).buildFuture();
        }
        for(String r : swsRoles.keySet()){
            dpRoles.add(r.toString()+":"+getRoleIntValue(swsRoles.get(r)));
        }
        GetSwitchRoleOutputBuilder gsrob = new GetSwitchRoleOutputBuilder();
        gsrob.setResponseCode(0L);
        gsrob.setResponseMessage(dpRoles);
        return RpcResultBuilder.success(gsrob.build()).buildFuture();
    }



    private int getRoleIntValue(String string) {
        // TODO
        // TODO
        // TODO
        // 0 NOCHANGE
        // 1 BECOME MASTAR
        // 2 BECOME SLAVE
        return 1;
    }

}