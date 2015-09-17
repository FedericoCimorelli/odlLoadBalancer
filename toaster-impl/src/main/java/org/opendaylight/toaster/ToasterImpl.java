package org.opendaylight.toaster;

// !!! NOTE: The imports must be in this order, or checkstyle will not pass!!!
// In Eclipse, use CONTROL+SHIFT+o or CMD+SHIFT+o (mac) to properly order imports

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;


// 3rd party imports
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

// MD-SAL APIs
import org.opendaylight.controller.md.sal.binding.api.DataBroker;
import org.opendaylight.controller.md.sal.binding.api.DataChangeListener;
import org.opendaylight.controller.md.sal.binding.api.ReadWriteTransaction;
import org.opendaylight.controller.md.sal.binding.api.WriteTransaction;
import org.opendaylight.controller.md.sal.common.api.data.AsyncDataBroker.DataChangeScope;
import org.opendaylight.controller.md.sal.common.api.data.AsyncDataChangeEvent;
import org.opendaylight.controller.md.sal.common.api.data.LogicalDatastoreType;
import org.opendaylight.controller.md.sal.common.api.data.OptimisticLockFailedException;
import org.opendaylight.controller.md.sal.common.api.data.TransactionCommitFailedException;
import org.opendaylight.controller.sal.binding.api.BindingAwareBroker.ProviderContext;
import org.opendaylight.controller.sal.binding.api.BindingAwareBroker.RpcRegistration;
import org.opendaylight.controller.sal.binding.api.BindingAwareProvider;
import org.opendaylight.controller.sal.binding.api.NotificationProviderService;
import org.opendaylight.yang.gen.v1.http.netconfcentral.org.ns.loadbalancer.rev091120.DisplayString;
import org.opendaylight.yang.gen.v1.http.netconfcentral.org.ns.loadbalancer.rev091120.Loadbalancer;
import org.opendaylight.yang.gen.v1.http.netconfcentral.org.ns.loadbalancer.rev091120.Loadbalancer.LoadbalancerStatus;
import org.opendaylight.yang.gen.v1.http.netconfcentral.org.ns.loadbalancer.rev091120.LoadbalancerBuilder;
import org.opendaylight.yang.gen.v1.http.netconfcentral.org.ns.loadbalancer.rev091120.LoadbalancerOutOfBreadBuilder;
import org.opendaylight.yang.gen.v1.http.netconfcentral.org.ns.loadbalancer.rev091120.LoadbalancerRestocked;
import org.opendaylight.yang.gen.v1.http.netconfcentral.org.ns.loadbalancer.rev091120.LoadbalancerRestockedBuilder;
// Interfaces generated from the toaster yang model
import org.opendaylight.yang.gen.v1.http.netconfcentral.org.ns.loadbalancer.rev091120.LoadbalancerService;
import org.opendaylight.yang.gen.v1.http.netconfcentral.org.ns.loadbalancer.rev091120.MakeToastInput;
import org.opendaylight.yang.gen.v1.http.netconfcentral.org.ns.loadbalancer.rev091120.RestockLoadbalancerInput;
import org.opendaylight.yang.gen.v1.urn.opendaylight.params.xml.ns.yang.toaster.impl.config.rev141210.ToasterImplRuntimeMXBean;
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




public class ToasterImpl implements BindingAwareProvider, LoadbalancerService, DataChangeListener, ToasterImplRuntimeMXBean, AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(ToasterImpl.class);

    private ProviderContext providerContext;

    private NotificationProviderService notificationService;
    private DataBroker dataService;

    private RpcRegistration<LoadbalancerService> rpcReg;
    private ListenerRegistration<DataChangeListener> dcReg;

    public static final InstanceIdentifier<Loadbalancer> LOADBALANCER_IID = InstanceIdentifier.builder(Loadbalancer.class).build();
    private static final DisplayString LOADBALANCER_MANUFACTURER = new DisplayString("Opendaylight Load Balancer");
    private static final DisplayString LOADBALANCER_MODEL_NUMBER = new DisplayString("Model 1 - Binding Aware");

    private final ExecutorService executor;

    // The following holds the Future for the current make toast task.
    // This is used to cancel the current toast.
    private final AtomicReference<Future<?>> currentMakeToastTask = new AtomicReference<>();

    private final AtomicLong amountOfBreadInStock = new AtomicLong( 100 );

    private final AtomicLong toastsMade = new AtomicLong(0);

    // Thread safe holder for our darkness multiplier.
    private final AtomicLong darknessFactor = new AtomicLong( 1000 );


    public ToasterImpl() {
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
     * ToasterService Methods
     *************************************************************************/

    /**
     * Restconf RPC call implemented from the ToasterService interface.
     * Cancels the current toast.
     * Implementation to be filled in a later chapter.
     * in postman, http://localhost:8181/restconf/operations/toaster:cancel-toast
     */
    @Override
    public Future<RpcResult<Void>> cancelToast() {
        LOG.info("cancelToast");
        Future<?> current = currentMakeToastTask.getAndSet( null );
        if( current != null ) {
            current.cancel( true );
        }

        // Always return success from the cancel toast call.
        return Futures.immediateFuture( RpcResultBuilder.<Void> success().build() );
    }

    /**
     * RestConf RPC call implemented from the ToasterService interface.
     * Attempts to make toast.
     * Implementation to be filled in a later chapter.
     * in postman, http://localhost:8181/restconf/operations/toaster:make-toast
     * { "input" : { "toaster:toasterDoneness" : "10", "toaster:toasterToastType":"wheat-bread" } }
     */
    @Override
    public Future<RpcResult<Void>> makeToast(final MakeToastInput input) {
        LOG.info("makeToast: {}", input);

        final SettableFuture<RpcResult<Void>> futureResult = SettableFuture.create();

        checkStatusAndMakeToast( input, futureResult, 2 );
        LOG.info("makeToast returning...");
        return futureResult;
    }

    /**
     * RestConf RPC call implemented from the ToasterService interface.
     * Restocks the bread for the toaster
     * Implementation to be filled in a later chapter.
     * in postman, http://localhost:8181/restconf/operations/toaster:restock-toaster
     * { "input" : { "toaster:amountOfBreadToStock" : "3" } }
     */
    @Override
    public Future<RpcResult<java.lang.Void>> restockLoadbalancer(final RestockLoadbalancerInput input) {
        LOG.info( "restockLoadbalancer: {}", input );
        amountOfBreadInStock.set( input.getAmountOfBreadToStock() );

        if( amountOfBreadInStock.get() > 0 ) {
            LoadbalancerRestocked reStockedNotification = new LoadbalancerRestockedBuilder().setAmountOfBread( input.getAmountOfBreadToStock() ).build();
            notificationService.publish( reStockedNotification );
        }
        return Futures.immediateFuture( RpcResultBuilder.<Void> success().build() );
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
            if( darkness != null )
            {
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
        return new LoadbalancerBuilder().setLoadbalancerManufacturer( LOADBALANCER_MANUFACTURER )
                                   .setLoadbalancerModelNumber( LOADBALANCER_MODEL_NUMBER )
                                   .setLoadbalancerStatus( status )
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
    private void checkStatusAndMakeToast( final MakeToastInput input,
            final SettableFuture<RpcResult<Void>> futureResult,
            final int tries ) {
        LOG.info( "checkStatusAndMakeToast");

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

                    LOG.debug( "Oops - already making toast!" );

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
                currentMakeToastTask.set( executor.submit( new MakeToastTask( input, futureResult ) ) );
            }

            @Override
            public void onFailure( final Throwable ex ) {
                if( ex instanceof OptimisticLockFailedException ) {

                    // Another thread is likely trying to make toast simultaneously and updated the
                    // status before us. Try reading the status again - if another make toast is
                    // now in progress, we should get ToasterStatus.Down and fail.

                    if( ( tries - 1 ) > 0 ) {
                        LOG.debug( "Got OptimisticLockFailedException - trying again" );

                        checkStatusAndMakeToast( input, futureResult, tries - 1 );
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

    private class MakeToastTask implements Callable<Void> {

        final MakeToastInput toastRequest;
        final SettableFuture<RpcResult<Void>> futureResult;

        public MakeToastTask( final MakeToastInput toastRequest,
                              final SettableFuture<RpcResult<Void>> futureResult ) {
            this.toastRequest = toastRequest;
            this.futureResult = futureResult;
        }

        @Override
        public Void call() {
            try
            {
                // make toast just sleeps for n seconds per doneness level.
                long darknessFactor = ToasterImpl.this.darknessFactor.get();
                Thread.sleep(darknessFactor * toastRequest.getLoadbalancerDoneness());

            }
            catch( InterruptedException e ) {
                LOG.info( "Interrupted while making the toast" );
            }

            toastsMade.incrementAndGet();

            amountOfBreadInStock.getAndDecrement();
            if( outOfBread() ) {
                LOG.info( "Loadbalancer is out of bread!" );

                notificationService.publish( new LoadbalancerOutOfBreadBuilder().build() );
            }

            // Set the Toaster status back to up - this essentially releases the toasting lock.
            // We can't clear the current toast task nor set the Future result until the
            // update has been committed so we pass a callback to be notified on completion.

            setLoadbalancerStatusUp( new Function<Boolean,Void>() {
                @Override
                public Void apply( final Boolean result ) {

                    currentMakeToastTask.set( null );

                    LOG.debug("Toast done");

                    futureResult.set( RpcResultBuilder.<Void>success().build() );

                    return null;
                }
            } );

            return null;
        }
    }

    /**
     * JMX RPC call implemented from the ToasterImplRuntimeMXBean interface.  Use jconcole to attach to karaf
     * to gain access to clearToastsMade and getToastsMade.  jconsole is run from the shell!
     */
    @Override
    public void clearToastsMade() {
        LOG.info( "clearToastsMade" );
        toastsMade.set( 0 );
    }

    /**
     * Accesssor method implemented from the ToasterImplRuntimeMXBean interface.
     */
    @Override
    public Long getToastsMade() {
        return toastsMade.get();
    }
}