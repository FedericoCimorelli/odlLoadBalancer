
package org.opendaylight.loadbalancer;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.SettableFuture;

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
import org.opendaylight.yang.gen.v1.http.netconfcentral.org.ns.loadbalancer.rev150901.Loadbalancer;
import org.opendaylight.yang.gen.v1.http.netconfcentral.org.ns.loadbalancer.rev150901.Loadbalancer.LoadbalancerStatus;
import org.opendaylight.yang.gen.v1.http.netconfcentral.org.ns.loadbalancer.rev150901.LoadbalancerBuilder;
import org.opendaylight.yang.gen.v1.http.netconfcentral.org.ns.loadbalancer.rev150901.LoadbalancerService;
import org.opendaylight.yang.gen.v1.http.netconfcentral.org.ns.loadbalancer.rev150901.StartLoadbalancerInput;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.NodeConnectorRemoved;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.NodeConnectorUpdated;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.NodeRemoved;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.NodeUpdated;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.Nodes;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.OpendaylightInventoryListener;
import org.opendaylight.yang.gen.v1.urn.opendaylight.packet.service.rev130709.PacketProcessingListener;
import org.opendaylight.yang.gen.v1.urn.opendaylight.packet.service.rev130709.PacketReceived;

// Yangtools methods to manipulate RPC DTOs
import org.opendaylight.yangtools.concepts.ListenerRegistration;
import org.opendaylight.yangtools.yang.binding.DataObject;
import org.opendaylight.yangtools.yang.binding.InstanceIdentifier;
import org.opendaylight.yangtools.yang.common.RpcResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class LoadbalancerImpl implements    BindingAwareProvider,
                                            DataChangeListener,
                                            AutoCloseable,
                                            LoadbalancerService,
                                            OpendaylightInventoryListener,
                                            PacketProcessingListener{

    private static final Logger LOG = LoggerFactory.getLogger(LoadbalancerImpl.class);

    private ProviderContext providerContext;
    private DataBroker dataService;
    private ListenerRegistration<DataChangeListener> dcReg;
    private BindingAwareBroker.RpcRegistration<LoadbalancerService> rpcReg;
    public static final InstanceIdentifier<Loadbalancer> LOADBALANCER_IID = InstanceIdentifier.builder(Loadbalancer.class).build();




    @Override
    public void close() throws Exception {
        dcReg.close();
        rpcReg.close();
        LOG.info("Registrations closed");
    }



    @Override
    public void onSessionInitiated(ProviderContext session) {
        this.providerContext = session;
        this.dataService = session.getSALService(DataBroker.class);
        // Register the DataChangeListener for Toaster's configuration subtree
        dcReg = dataService.registerDataChangeListener( LogicalDatastoreType.CONFIGURATION,
                                                        LOADBALANCER_IID,
                                                        this,
                                                        DataChangeScope.SUBTREE );
        // Register the RPC Service
        rpcReg = session.addRpcImplementation(LoadbalancerService.class, this);
        // Initialize operational and default config data in MD-SAL data store
        initToasterOperational();
        initToasterConfiguration();
        LOG.info("onSessionInitiated: initialization done");
    }



    @Override
    public void onDataChanged( final AsyncDataChangeEvent<InstanceIdentifier<?>, DataObject> change ) {
        DataObject dataObject = change.getUpdatedSubtree();
        if( dataObject instanceof Loadbalancer ){
            Loadbalancer loadbalancer = (Loadbalancer) dataObject;
            LOG.info("onDataChanged - new Loadbalancer config: {}", loadbalancer);
        } else {
            LOG.warn("onDataChanged - not instance of Loadbalancer {}", dataObject);
        }
    }


    @Override
    public void onNodeConnectorRemoved(NodeConnectorRemoved arg0) {
        String s = "LOADBALANCER"+arg0.toString();
        LOG.info(s);
    }

    @Override
    public void onNodeConnectorUpdated(NodeConnectorUpdated arg0) {
        String s = "LOADBALANCER"+arg0.toString();
        LOG.info(s);
    }

    @Override
    public void onNodeRemoved(NodeRemoved arg0) {
        String s = "LOADBALANCER"+arg0.toString();
        LOG.info(s);
    }

    @Override
    public void onNodeUpdated(NodeUpdated arg0) {
        String s = "LOADBALANCER"+arg0.toString();
        LOG.info(s);
    }

    @Override
    public void onPacketReceived(PacketReceived arg0) {
        String s = "LOADBALANCER"+arg0.toString();
        LOG.info(s);
    }





    private void testAccessDataInMdSal() {
        LOG.info("Test access data in MD-SAL - start");
        InstanceIdentifier<Nodes> nI = InstanceIdentifier.builder(Nodes.class).build();
        ReadOnlyTransaction readTransaction = dataService.newReadOnlyTransaction();
        try {
            Optional<Nodes> optionalData = readTransaction.read(LogicalDatastoreType.CONFIGURATION, nI).get();
            if (optionalData.isPresent()) {
                Nodes ns = (Nodes) optionalData.get();
                LOG.info(ns.toString());
            }
        } catch (ExecutionException | InterruptedException e) {
            readTransaction.close();
            String sLog = "Test access data in MD-SAL - error: "+e.getMessage();
            LOG.info(sLog);
        }
        LOG.info("Test access data in MD-SAL - end");
    }




    @Override
    public Future<RpcResult<Void>> startLoadbalancer(StartLoadbalancerInput input) {
        LOG.info("Test start loadbalancer service - start");
        //TESTING METHODS
        testAccessDataInMdSal();
        LOG.info("Test start loadbalancer service - end");
        final SettableFuture<RpcResult<Void>> futureResult = SettableFuture.create();
        return futureResult;
    }





    private void initToasterOperational() {
        Loadbalancer loadbalancer = new LoadbalancerBuilder()
                .setDarknessFactor(1000l)
                .setLoadbalancerStatus(LoadbalancerStatus.Down)
                .build();
        WriteTransaction tx = dataService.newWriteOnlyTransaction();
        tx.put(LogicalDatastoreType.OPERATIONAL, LOADBALANCER_IID, loadbalancer);
        Futures.addCallback(tx.submit(), new FutureCallback<Void>() {
            @Override
            public void onSuccess(final Void result) {
                LOG.info("initToasterOperational: transaction succeeded");
            }
            @Override
            public void onFailure(final Throwable t) {
                LOG.error("initToasterOperational: transaction failed");
            }
        });
        LOG.info("initToasterOperational: operational status populated: {}", loadbalancer);
    }




    private void initToasterConfiguration() {
        Loadbalancer toaster = new LoadbalancerBuilder().setDarknessFactor((long)1000).build();
        WriteTransaction tx = dataService.newWriteOnlyTransaction();
        tx.put(LogicalDatastoreType.CONFIGURATION, LOADBALANCER_IID, toaster);
        tx.submit();
        LOG.info("initToasterConfiguration: default config populated: {}", toaster);
    }

}