package ca.digilogue.xp.grpc;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 * <pre>
 **
 * OHLCV (Open, High, Low, Close, Volume) gRPC Service
 * Provides access to real-time OHLCV candle data.
 * </pre>
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.64.0)",
    comments = "Source: ohlcv_service.proto")
@io.grpc.stub.annotations.GrpcGenerated
public final class OhlcvServiceGrpc {

  private OhlcvServiceGrpc() {}

  public static final java.lang.String SERVICE_NAME = "ca.digilogue.xp.grpc.OhlcvService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<ca.digilogue.xp.grpc.OhlcvServiceProto.GetLatestCandleRequest,
      ca.digilogue.xp.grpc.OhlcvServiceProto.OhlcvCandleResponse> getGetLatestCandleMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetLatestCandle",
      requestType = ca.digilogue.xp.grpc.OhlcvServiceProto.GetLatestCandleRequest.class,
      responseType = ca.digilogue.xp.grpc.OhlcvServiceProto.OhlcvCandleResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<ca.digilogue.xp.grpc.OhlcvServiceProto.GetLatestCandleRequest,
      ca.digilogue.xp.grpc.OhlcvServiceProto.OhlcvCandleResponse> getGetLatestCandleMethod() {
    io.grpc.MethodDescriptor<ca.digilogue.xp.grpc.OhlcvServiceProto.GetLatestCandleRequest, ca.digilogue.xp.grpc.OhlcvServiceProto.OhlcvCandleResponse> getGetLatestCandleMethod;
    if ((getGetLatestCandleMethod = OhlcvServiceGrpc.getGetLatestCandleMethod) == null) {
      synchronized (OhlcvServiceGrpc.class) {
        if ((getGetLatestCandleMethod = OhlcvServiceGrpc.getGetLatestCandleMethod) == null) {
          OhlcvServiceGrpc.getGetLatestCandleMethod = getGetLatestCandleMethod =
              io.grpc.MethodDescriptor.<ca.digilogue.xp.grpc.OhlcvServiceProto.GetLatestCandleRequest, ca.digilogue.xp.grpc.OhlcvServiceProto.OhlcvCandleResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetLatestCandle"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  ca.digilogue.xp.grpc.OhlcvServiceProto.GetLatestCandleRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  ca.digilogue.xp.grpc.OhlcvServiceProto.OhlcvCandleResponse.getDefaultInstance()))
              .setSchemaDescriptor(new OhlcvServiceMethodDescriptorSupplier("GetLatestCandle"))
              .build();
        }
      }
    }
    return getGetLatestCandleMethod;
  }

  private static volatile io.grpc.MethodDescriptor<ca.digilogue.xp.grpc.OhlcvServiceProto.StreamAllLiveCandlesRequest,
      ca.digilogue.xp.grpc.OhlcvServiceProto.AllCandlesResponse> getStreamAllLiveCandlesMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "StreamAllLiveCandles",
      requestType = ca.digilogue.xp.grpc.OhlcvServiceProto.StreamAllLiveCandlesRequest.class,
      responseType = ca.digilogue.xp.grpc.OhlcvServiceProto.AllCandlesResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
  public static io.grpc.MethodDescriptor<ca.digilogue.xp.grpc.OhlcvServiceProto.StreamAllLiveCandlesRequest,
      ca.digilogue.xp.grpc.OhlcvServiceProto.AllCandlesResponse> getStreamAllLiveCandlesMethod() {
    io.grpc.MethodDescriptor<ca.digilogue.xp.grpc.OhlcvServiceProto.StreamAllLiveCandlesRequest, ca.digilogue.xp.grpc.OhlcvServiceProto.AllCandlesResponse> getStreamAllLiveCandlesMethod;
    if ((getStreamAllLiveCandlesMethod = OhlcvServiceGrpc.getStreamAllLiveCandlesMethod) == null) {
      synchronized (OhlcvServiceGrpc.class) {
        if ((getStreamAllLiveCandlesMethod = OhlcvServiceGrpc.getStreamAllLiveCandlesMethod) == null) {
          OhlcvServiceGrpc.getStreamAllLiveCandlesMethod = getStreamAllLiveCandlesMethod =
              io.grpc.MethodDescriptor.<ca.digilogue.xp.grpc.OhlcvServiceProto.StreamAllLiveCandlesRequest, ca.digilogue.xp.grpc.OhlcvServiceProto.AllCandlesResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "StreamAllLiveCandles"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  ca.digilogue.xp.grpc.OhlcvServiceProto.StreamAllLiveCandlesRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  ca.digilogue.xp.grpc.OhlcvServiceProto.AllCandlesResponse.getDefaultInstance()))
              .setSchemaDescriptor(new OhlcvServiceMethodDescriptorSupplier("StreamAllLiveCandles"))
              .build();
        }
      }
    }
    return getStreamAllLiveCandlesMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static OhlcvServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<OhlcvServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<OhlcvServiceStub>() {
        @java.lang.Override
        public OhlcvServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new OhlcvServiceStub(channel, callOptions);
        }
      };
    return OhlcvServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static OhlcvServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<OhlcvServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<OhlcvServiceBlockingStub>() {
        @java.lang.Override
        public OhlcvServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new OhlcvServiceBlockingStub(channel, callOptions);
        }
      };
    return OhlcvServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static OhlcvServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<OhlcvServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<OhlcvServiceFutureStub>() {
        @java.lang.Override
        public OhlcvServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new OhlcvServiceFutureStub(channel, callOptions);
        }
      };
    return OhlcvServiceFutureStub.newStub(factory, channel);
  }

  /**
   * <pre>
   **
   * OHLCV (Open, High, Low, Close, Volume) gRPC Service
   * Provides access to real-time OHLCV candle data.
   * </pre>
   */
  public interface AsyncService {

    /**
     * <pre>
     **
     * Gets the latest OHLCV candle for a given symbol.
     * 
     * &#64;param request Contains the trading symbol (e.g., "MEGA-USD")
     * &#64;return The latest OHLCV candle data for the symbol
     * </pre>
     */
    default void getLatestCandle(ca.digilogue.xp.grpc.OhlcvServiceProto.GetLatestCandleRequest request,
        io.grpc.stub.StreamObserver<ca.digilogue.xp.grpc.OhlcvServiceProto.OhlcvCandleResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetLatestCandleMethod(), responseObserver);
    }

    /**
     * <pre>
     **
     * Streams all live OHLCV candles from all active generators in real-time.
     * Sends a collection of all current candles every second.
     * 
     * &#64;param request Empty request (no parameters needed)
     * &#64;return Stream of AllCandlesResponse containing all current candles
     * </pre>
     */
    default void streamAllLiveCandles(ca.digilogue.xp.grpc.OhlcvServiceProto.StreamAllLiveCandlesRequest request,
        io.grpc.stub.StreamObserver<ca.digilogue.xp.grpc.OhlcvServiceProto.AllCandlesResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getStreamAllLiveCandlesMethod(), responseObserver);
    }
  }

  /**
   * Base class for the server implementation of the service OhlcvService.
   * <pre>
   **
   * OHLCV (Open, High, Low, Close, Volume) gRPC Service
   * Provides access to real-time OHLCV candle data.
   * </pre>
   */
  public static abstract class OhlcvServiceImplBase
      implements io.grpc.BindableService, AsyncService {

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return OhlcvServiceGrpc.bindService(this);
    }
  }

  /**
   * A stub to allow clients to do asynchronous rpc calls to service OhlcvService.
   * <pre>
   **
   * OHLCV (Open, High, Low, Close, Volume) gRPC Service
   * Provides access to real-time OHLCV candle data.
   * </pre>
   */
  public static final class OhlcvServiceStub
      extends io.grpc.stub.AbstractAsyncStub<OhlcvServiceStub> {
    private OhlcvServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected OhlcvServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new OhlcvServiceStub(channel, callOptions);
    }

    /**
     * <pre>
     **
     * Gets the latest OHLCV candle for a given symbol.
     * 
     * &#64;param request Contains the trading symbol (e.g., "MEGA-USD")
     * &#64;return The latest OHLCV candle data for the symbol
     * </pre>
     */
    public void getLatestCandle(ca.digilogue.xp.grpc.OhlcvServiceProto.GetLatestCandleRequest request,
        io.grpc.stub.StreamObserver<ca.digilogue.xp.grpc.OhlcvServiceProto.OhlcvCandleResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetLatestCandleMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     **
     * Streams all live OHLCV candles from all active generators in real-time.
     * Sends a collection of all current candles every second.
     * 
     * &#64;param request Empty request (no parameters needed)
     * &#64;return Stream of AllCandlesResponse containing all current candles
     * </pre>
     */
    public void streamAllLiveCandles(ca.digilogue.xp.grpc.OhlcvServiceProto.StreamAllLiveCandlesRequest request,
        io.grpc.stub.StreamObserver<ca.digilogue.xp.grpc.OhlcvServiceProto.AllCandlesResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncServerStreamingCall(
          getChannel().newCall(getStreamAllLiveCandlesMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * A stub to allow clients to do synchronous rpc calls to service OhlcvService.
   * <pre>
   **
   * OHLCV (Open, High, Low, Close, Volume) gRPC Service
   * Provides access to real-time OHLCV candle data.
   * </pre>
   */
  public static final class OhlcvServiceBlockingStub
      extends io.grpc.stub.AbstractBlockingStub<OhlcvServiceBlockingStub> {
    private OhlcvServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected OhlcvServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new OhlcvServiceBlockingStub(channel, callOptions);
    }

    /**
     * <pre>
     **
     * Gets the latest OHLCV candle for a given symbol.
     * 
     * &#64;param request Contains the trading symbol (e.g., "MEGA-USD")
     * &#64;return The latest OHLCV candle data for the symbol
     * </pre>
     */
    public ca.digilogue.xp.grpc.OhlcvServiceProto.OhlcvCandleResponse getLatestCandle(ca.digilogue.xp.grpc.OhlcvServiceProto.GetLatestCandleRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetLatestCandleMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Streams all live OHLCV candles from all active generators in real-time.
     * Sends a collection of all current candles every second.
     * 
     * &#64;param request Empty request (no parameters needed)
     * &#64;return Stream of AllCandlesResponse containing all current candles
     * </pre>
     */
    public java.util.Iterator<ca.digilogue.xp.grpc.OhlcvServiceProto.AllCandlesResponse> streamAllLiveCandles(
        ca.digilogue.xp.grpc.OhlcvServiceProto.StreamAllLiveCandlesRequest request) {
      return io.grpc.stub.ClientCalls.blockingServerStreamingCall(
          getChannel(), getStreamAllLiveCandlesMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do ListenableFuture-style rpc calls to service OhlcvService.
   * <pre>
   **
   * OHLCV (Open, High, Low, Close, Volume) gRPC Service
   * Provides access to real-time OHLCV candle data.
   * </pre>
   */
  public static final class OhlcvServiceFutureStub
      extends io.grpc.stub.AbstractFutureStub<OhlcvServiceFutureStub> {
    private OhlcvServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected OhlcvServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new OhlcvServiceFutureStub(channel, callOptions);
    }

    /**
     * <pre>
     **
     * Gets the latest OHLCV candle for a given symbol.
     * 
     * &#64;param request Contains the trading symbol (e.g., "MEGA-USD")
     * &#64;return The latest OHLCV candle data for the symbol
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<ca.digilogue.xp.grpc.OhlcvServiceProto.OhlcvCandleResponse> getLatestCandle(
        ca.digilogue.xp.grpc.OhlcvServiceProto.GetLatestCandleRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetLatestCandleMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_GET_LATEST_CANDLE = 0;
  private static final int METHODID_STREAM_ALL_LIVE_CANDLES = 1;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final AsyncService serviceImpl;
    private final int methodId;

    MethodHandlers(AsyncService serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_GET_LATEST_CANDLE:
          serviceImpl.getLatestCandle((ca.digilogue.xp.grpc.OhlcvServiceProto.GetLatestCandleRequest) request,
              (io.grpc.stub.StreamObserver<ca.digilogue.xp.grpc.OhlcvServiceProto.OhlcvCandleResponse>) responseObserver);
          break;
        case METHODID_STREAM_ALL_LIVE_CANDLES:
          serviceImpl.streamAllLiveCandles((ca.digilogue.xp.grpc.OhlcvServiceProto.StreamAllLiveCandlesRequest) request,
              (io.grpc.stub.StreamObserver<ca.digilogue.xp.grpc.OhlcvServiceProto.AllCandlesResponse>) responseObserver);
          break;
        default:
          throw new AssertionError();
      }
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        default:
          throw new AssertionError();
      }
    }
  }

  public static final io.grpc.ServerServiceDefinition bindService(AsyncService service) {
    return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
        .addMethod(
          getGetLatestCandleMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              ca.digilogue.xp.grpc.OhlcvServiceProto.GetLatestCandleRequest,
              ca.digilogue.xp.grpc.OhlcvServiceProto.OhlcvCandleResponse>(
                service, METHODID_GET_LATEST_CANDLE)))
        .addMethod(
          getStreamAllLiveCandlesMethod(),
          io.grpc.stub.ServerCalls.asyncServerStreamingCall(
            new MethodHandlers<
              ca.digilogue.xp.grpc.OhlcvServiceProto.StreamAllLiveCandlesRequest,
              ca.digilogue.xp.grpc.OhlcvServiceProto.AllCandlesResponse>(
                service, METHODID_STREAM_ALL_LIVE_CANDLES)))
        .build();
  }

  private static abstract class OhlcvServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    OhlcvServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return ca.digilogue.xp.grpc.OhlcvServiceProto.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("OhlcvService");
    }
  }

  private static final class OhlcvServiceFileDescriptorSupplier
      extends OhlcvServiceBaseDescriptorSupplier {
    OhlcvServiceFileDescriptorSupplier() {}
  }

  private static final class OhlcvServiceMethodDescriptorSupplier
      extends OhlcvServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final java.lang.String methodName;

    OhlcvServiceMethodDescriptorSupplier(java.lang.String methodName) {
      this.methodName = methodName;
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.MethodDescriptor getMethodDescriptor() {
      return getServiceDescriptor().findMethodByName(methodName);
    }
  }

  private static volatile io.grpc.ServiceDescriptor serviceDescriptor;

  public static io.grpc.ServiceDescriptor getServiceDescriptor() {
    io.grpc.ServiceDescriptor result = serviceDescriptor;
    if (result == null) {
      synchronized (OhlcvServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new OhlcvServiceFileDescriptorSupplier())
              .addMethod(getGetLatestCandleMethod())
              .addMethod(getStreamAllLiveCandlesMethod())
              .build();
        }
      }
    }
    return result;
  }
}
