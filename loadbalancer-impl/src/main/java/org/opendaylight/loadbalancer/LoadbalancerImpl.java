
package org.opendaylight.loadbalancer;

//!!! NOTE: The imports must be in this order, or checkstyle will not pass!!!
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.SettableFuture;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.opendaylight.controller.md.sal.binding.api.DataBroker;
import org.opendaylight.controller.md.sal.binding.api.DataChangeListener;
import org.opendaylight.controller.md.sal.binding.api.ReadOnlyTransaction;
import org.opendaylight.controller.md.sal.binding.api.WriteTransaction;
import org.opendaylight.controller.md.sal.common.api.data.AsyncDataBroker.DataChangeScope;
import org.opendaylight.controller.md.sal.common.api.data.AsyncDataChangeEvent;
import org.opendaylight.controller.md.sal.common.api.data.LogicalDatastoreType;
import org.opendaylight.controller.sal.binding.api.BindingAwareBroker.ProviderContext;
import org.opendaylight.controller.sal.binding.api.BindingAwareBroker.RpcRegistration;
import org.opendaylight.controller.sal.binding.api.BindingAwareProvider;
import org.opendaylight.controller.sal.binding.api.NotificationProviderService;
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
import org.opendaylight.yangtools.yang.common.RpcError;
import org.opendaylight.yangtools.yang.common.RpcError.ErrorType;
import org.opendaylight.yangtools.yang.common.RpcResult;
import org.opendaylight.yangtools.yang.common.RpcResultBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class LoadbalancerImpl implements BindingAwareProvider,
                                            LoadbalancerService,
                                            DataChangeListener,
                                            AutoCloseable,
                                            OpendaylightInventoryListener,
                                            PacketProcessingListener{

    private static final String TAG = "LOADBALANCER";
    private static final Logger LOG = LoggerFactory.getLogger(LoadbalancerImpl.class);
    private ProviderContext providerContext;
    private NotificationProviderService notificationService;
    private DataBroker dataService;
    private RpcRegistration<LoadbalancerService> rpcReg;
    private ListenerRegistration<DataChangeListener> dcReg;
    public static final InstanceIdentifier<Loadbalancer> LOADBALANCER_IID = InstanceIdentifier.builder(Loadbalancer.class).build();
    private final ExecutorService executor;
    private final AtomicReference<Future<?>> currentMakeLbTask = new AtomicReference<>();
    private final AtomicLong amountOfBreadInStock = new AtomicLong( 100 );
    private final AtomicLong lbsMade = new AtomicLong(0);
    private final AtomicLong darknessFactor = new AtomicLong( 1000 );


    public LoadbalancerImpl() {
        executor = Executors.newFixedThreadPool(1);
    }


    /**************************************************************************
     * AutoCloseable Method
     *************************************************************************/
    /**
     * Called when MD-SAL closes the active session. Cleanup is performed, i.e.
     * all active registrations with MD-SAL are closed,
     */
    @Override
    public void close() throws Exception {
        // When we close this service we need to shutdown our executor!
        executor.shutdown();
        // Delete toaster operational data from the MD-SAL data store
        WriteTransaction tx = dataService.newWriteOnlyTransaction();
        tx.delete(LogicalDatastoreType.OPERATIONAL,LOADBALANCER_IID);
        Futures.addCallback( tx.submit(), new FutureCallback<Void>() {
            @Override
            public void onSuccess( final Void result ) {
                LOG.debug( "Delete Loadbalancer commit result: {}", result );
            }
            @Override
            public void onFailure( final Throwable t ) {
                LOG.error( "Delete of Loadbalancer failed", t );
            }
        } );
        // Close active registrations
        rpcReg.close();
        dcReg.close();
        LOG.info("LoadbalancerImpl: registrations closed");
    }



    /**************************************************************************
     * BindingAwareProvider Methods
     *************************************************************************/
    @Override
    public void onSessionInitiated(ProviderContext session) {
        this.providerContext = session;
        this.notificationService = session.getSALService(NotificationProviderService.class);
        this.dataService = session.getSALService(DataBroker.class);
        // Register the RPC Service
        rpcReg = session.addRpcImplementation(LoadbalancerService.class, this);
        // Register the DataChangeListener for Toaster's configuration subtree
        dcReg = dataService.registerDataChangeListener( LogicalDatastoreType.CONFIGURATION,
                                                        LOADBALANCER_IID,
                                                        this,
                                                        DataChangeScope.SUBTREE );
        // Initialize operational and default config data in MD-SAL data store
        initLoadbalancerOperational();
        initLoadbalancerConfiguration();
        LOG.info("onSessionInitiated: initialization done");
    }






    /**************************************************************************
     * DataChangeListener Methods
     *************************************************************************/
    /**
     * Receives data change events on toaster's configuration subtree. Invoked
     * when data is written into the toaster's configuration subtree in the
     * MD-SAL data store.
     */
    @Override
    public void onDataChanged( final AsyncDataChangeEvent<InstanceIdentifier<?>, DataObject> change ) {
        DataObject dataObject = change.getUpdatedSubtree();
        if( dataObject instanceof Loadbalancer )
        {
            Loadbalancer loadbalancer = (Loadbalancer) dataObject;
            Long darkness = loadbalancer.getDarknessFactor();
            if( darkness != null ){
                darknessFactor.set( darkness );
            }
            LOG.info("onDataChanged - new Loadbalancer config: {}", loadbalancer);
        }
    }





    /**************************************************************************
     * ToasterImpl Private Methods
     *************************************************************************/

    /**
     * Populates toaster's initial operational data into the MD-SAL operational
     * data store.
     * Note - we are simulating a device whose manufacture and model are fixed
     * (embedded) into the hardware. / This is why the manufacture and model
     * number are hardcoded
     */
    private void initLoadbalancerOperational() {
        // Build the initial toaster operational data
        Loadbalancer loadbalancer = buildLoadbalancer(LoadbalancerStatus.Up);

        // Put the toaster operational data into the MD-SAL data store
        WriteTransaction tx = dataService.newWriteOnlyTransaction();
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






    /**
     * Populates toaster's default config data into the MD-SAL configuration
     * data store.
     */
    private void initLoadbalancerConfiguration() {
        // Build the default toaster config data
        Loadbalancer loadbalancer = new LoadbalancerBuilder().setDarknessFactor(darknessFactor.get())
                .build();

        // Place default config data in data store tree
        WriteTransaction tx = dataService.newWriteOnlyTransaction();
        tx.put(LogicalDatastoreType.CONFIGURATION, LOADBALANCER_IID, loadbalancer);

        Futures.addCallback(tx.submit(), new FutureCallback<Void>() {
            @Override
            public void onSuccess(final Void result) {
                LOG.info("initLoadbalancerConfiguration: transaction succeeded");
            }

            @Override
            public void onFailure(final Throwable t) {
                LOG.error("initLoadbalancerConfiguration: transaction failed");
            }
        });

        LOG.info("initLoadbalancerConfiguration: default config populated: {}", loadbalancer);
    }

    private RpcError makeLoadbalancerOutOfBreadError() {
        return RpcResultBuilder.newError( ErrorType.APPLICATION, "resource-denied",
                "Loadbalancer is out of bread", "out-of-stock", null, null );
    }

    private RpcError makeLoadbalancerInUseError() {
        return RpcResultBuilder.newWarning( ErrorType.APPLICATION, "in-use",
                "Loadbalancer is busy", null, null, null );
    }

    private Loadbalancer buildLoadbalancer( final LoadbalancerStatus status ) {
        return new LoadbalancerBuilder().setLoadbalancerStatus( status )
                                   .build();
    }

    private void setLoadbalancerStatusUp( final Function<Boolean,Void> resultCallback ) {

        WriteTransaction tx = dataService.newWriteOnlyTransaction();
        tx.put( LogicalDatastoreType.OPERATIONAL,LOADBALANCER_IID, buildLoadbalancer( LoadbalancerStatus.Up ) );

        Futures.addCallback( tx.submit(), new FutureCallback<Void>() {
            @Override
            public void onSuccess( final Void result ) {
                notifyCallback( true );
            }

            @Override
            public void onFailure( final Throwable t ) {
                // We shouldn't get an OptimisticLockFailedException (or any ex) as no
                // other component should be updating the operational state.
                LOG.error( "Failed to update  status", t );

                notifyCallback( false );
            }

            void notifyCallback( final boolean result ) {
                if( resultCallback != null ) {
                    resultCallback.apply( result );
                }
            }
        } );
    }




    private boolean outOfBread()
    {
        return amountOfBreadInStock.get() == 0;
    }





    /**
     * Read the ToasterStatus and, if currently Up, try to write the status to
     * Down. If that succeeds, then we essentially have an exclusive lock and
     * can proceed to make toast.
     */
    /*private void checkStatusAndMakeLb( final MakeLbInput input,
            final SettableFuture<RpcResult<Void>> futureResult,
            final int tries ) {
        LOG.info( "checkStatusAndMakeLb");

        final ReadWriteTransaction tx = dataService.newReadWriteTransaction();
        ListenableFuture<Optional<Loadbalancer>> readFuture =
            tx.read( LogicalDatastoreType.OPERATIONAL, LOADBALANCER_IID );

        final ListenableFuture<Void> commitFuture =
            Futures.transform( readFuture, new AsyncFunction<Optional<Loadbalancer>,Void>() {

                @Override
                public ListenableFuture<Void> apply(
                        final Optional<Loadbalancer> loadbalancerData ) throws Exception {

                    LoadbalancerStatus loadbalancerStatus = LoadbalancerStatus.Up;
                    if( loadbalancerData.isPresent() ) {
                        loadbalancerStatus = loadbalancerData.get().getLoadbalancerStatus();
                    }

                    LOG.debug( "Read Loadbalancer status: {}", loadbalancerStatus );

                    if( loadbalancerStatus == LoadbalancerStatus.Up ) {
                        if( outOfBread() ) {
                            LOG.debug( "Loadbalancer is out of bread" );
                            return Futures.immediateFailedCheckedFuture(
                                    new TransactionCommitFailedException( "", makeLoadbalancerOutOfBreadError() ) );
                        }

                        LOG.debug( "Setting Loadbalancer status to Down" );

                        // We're not currently making toast - try to update the status to Down
                        // to indicate we're going to make toast. This acts as a lock to prevent
                        // concurrent toasting.
                        tx.put( LogicalDatastoreType.OPERATIONAL, LOADBALANCER_IID,
                                buildLoadbalancer( LoadbalancerStatus.Down ) );
                        return tx.submit();
                    }

                    LOG.debug( "Oops - already making lb!" );

                    // Return an error since we are already making toast. This will get
                    // propagated to the commitFuture below which will interpret the null
                    // TransactionStatus in the RpcResult as an error condition.
                    return Futures.immediateFailedCheckedFuture(
                            new TransactionCommitFailedException( "", makeLoadbalancerInUseError() ) );
                }
            } );

        Futures.addCallback( commitFuture, new FutureCallback<Void>() {
            @Override
            public void onSuccess( final Void result ) {
                // OK to make toast
                currentMakeLbTask.set( executor.submit( new MakeLbTask( input, futureResult ) ) );
            }

            @Override
            public void onFailure( final Throwable ex ) {
                if( ex instanceof OptimisticLockFailedException ) {

                    // Another thread is likely trying to make toast simultaneously and updated the
                    // status before us. Try reading the status again - if another make toast is
                    // now in progress, we should get ToasterStatus.Down and fail.

                    if( ( tries - 1 ) > 0 ) {
                        LOG.debug( "Got OptimisticLockFailedException - trying again" );

                        checkStatusAndMakeLb( input, futureResult, tries - 1 );
                    }
                    else {
                        futureResult.set( RpcResultBuilder.<Void> failed()
                                .withError( ErrorType.APPLICATION, ex.getMessage() ).build() );
                    }

                } else {

                    LOG.debug( "Failed to commit Loadbalancer status", ex );

                    // Probably already making toast.
                    futureResult.set( RpcResultBuilder.<Void> failed()
                            .withRpcErrors( ((TransactionCommitFailedException)ex).getErrorList() )
                            .build() );
                }
            }
        } );
    }
*/
   /* private class MakeLbTask implements Callable<Void> {
        final MakeLbInput lbRequest;
        final SettableFuture<RpcResult<Void>> futureResult;
        public MakeLbTask( final MakeLbInput lbRequest, final SettableFuture<RpcResult<Void>> futureResult ) {
            this.lbRequest = lbRequest;
            this.futureResult = futureResult;
        }
        @Override
        public Void call() {
            try{
                long darknessFactor = LoadbalancerImpl.this.darknessFactor.get();
                Thread.sleep(darknessFactor * lbRequest.getLoadbalancerDoneness());

            }
            catch( InterruptedException e ) {
                LOG.info( "Interrupted while making the lb" );
            }
            lbsMade.incrementAndGet();
            amountOfBreadInStock.getAndDecrement();
            if( outOfBread() ) {
                LOG.info( "Loadbalancer is out of bread!" );
                notificationService.publish( new LoadbalancerOutOfBreadBuilder().build() );
            }
            setLoadbalancerStatusUp( new Function<Boolean,Void>() {
                @Override
                public Void apply( final Boolean result ) {
                    currentMakeLbTask.set( null );
                    LOG.debug("Lb done");
                    futureResult.set( RpcResultBuilder.<Void>success().build() );
                    return null;
                }
            } );
            return null;
        }
    }
*/

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
        LOG.info(TAG, "Test access data in MD-SAL - start");
        InstanceIdentifier<Nodes> nI = InstanceIdentifier.builder(Nodes.class).build();
        ReadOnlyTransaction readTransaction = dataService.newReadOnlyTransaction();
        try {
            Optional<Nodes> optionalData = readTransaction.read(LogicalDatastoreType.CONFIGURATION, nI).get();
            if (optionalData.isPresent()) {
                Nodes ns = (Nodes) optionalData.get();
                LOG.info(TAG, ns.toString());
            }
        } catch (ExecutionException | InterruptedException e) {
            readTransaction.close();
            LOG.info(TAG, "Test access data in MD-SAL - error: "+e.getMessage());
        }
        LOG.info(TAG, "Test access data in MD-SAL - end");
    }


    @Override
    public Future<RpcResult<Void>> startLoadbalancer(StartLoadbalancerInput input) {
        LOG.info(TAG, "Test start loadbalancer service - start");
        //TESTING METHODS
        testAccessDataInMdSal();
        LOG.info(TAG, "Test start loadbalancer service - end");
        final SettableFuture<RpcResult<Void>> futureResult = SettableFuture.create();
        return futureResult;
    }


}