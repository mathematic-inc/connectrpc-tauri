///Shorthand for `OwnedView<GreetRequestView<'static>>`.
pub type OwnedGreetRequestView = ::buffa::view::OwnedView<
    __buffa::view::GreetRequestView<'static>,
>;
///Shorthand for `OwnedView<GreetResponseView<'static>>`.
pub type OwnedGreetResponseView = ::buffa::view::OwnedView<
    __buffa::view::GreetResponseView<'static>,
>;
///Shorthand for `OwnedView<GreetManyRequestView<'static>>`.
pub type OwnedGreetManyRequestView = ::buffa::view::OwnedView<
    __buffa::view::GreetManyRequestView<'static>,
>;
impl ::connectrpc::Encodable<GreetResponse> for __buffa::view::GreetResponseView<'_> {
    fn encode(
        &self,
        codec: ::connectrpc::CodecFormat,
    ) -> ::std::result::Result<::buffa::bytes::Bytes, ::connectrpc::ConnectError> {
        ::connectrpc::__codegen::encode_view_body(self, codec)
    }
}
impl ::connectrpc::Encodable<GreetResponse>
for ::buffa::view::OwnedView<__buffa::view::GreetResponseView<'static>> {
    fn encode(
        &self,
        codec: ::connectrpc::CodecFormat,
    ) -> ::std::result::Result<::buffa::bytes::Bytes, ::connectrpc::ConnectError> {
        ::connectrpc::__codegen::encode_view_body(self.reborrow(), codec)
    }
    /// An `OwnedView` still holds the buffer it was decoded from, so
    /// its large fields can be handed to the response body by
    /// reference count instead of copied. The bare view impl above
    /// cannot do this: it has borrows but no buffer to name.
    fn encode_segments(
        &self,
        codec: ::connectrpc::CodecFormat,
    ) -> ::std::result::Result<::connectrpc::EncodedBody, ::connectrpc::ConnectError> {
        ::connectrpc::__codegen::encode_view_body_segments(
            self.reborrow(),
            self.bytes(),
            codec,
        )
    }
}
/// Full service name for this service.
pub const GREET_SERVICE_SERVICE_NAME: &str = "greet.v1.GreetService";
/// Static [`Spec`](::connectrpc::Spec) for the `Greet` RPC, as seen by the server; the generated client passes it with [`origin`](::connectrpc::Spec::origin) `Client` (compare across sides with [`Spec::same_method`](::connectrpc::Spec::same_method)).
pub const GREET_SERVICE_GREET_SPEC: ::connectrpc::Spec = ::connectrpc::Spec::server(
        "/greet.v1.GreetService/Greet",
        ::connectrpc::StreamType::Unary,
    )
    .with_idempotency_level(::connectrpc::IdempotencyLevel::Unknown);
/// Static [`Spec`](::connectrpc::Spec) for the `GreetMany` RPC, as seen by the server; the generated client passes it with [`origin`](::connectrpc::Spec::origin) `Client` (compare across sides with [`Spec::same_method`](::connectrpc::Spec::same_method)).
pub const GREET_SERVICE_GREET_MANY_SPEC: ::connectrpc::Spec = ::connectrpc::Spec::server(
        "/greet.v1.GreetService/GreetMany",
        ::connectrpc::StreamType::ServerStream,
    )
    .with_idempotency_level(::connectrpc::IdempotencyLevel::Unknown);
/// Static [`Spec`](::connectrpc::Spec) for the `GreetAll` RPC, as seen by the server; the generated client passes it with [`origin`](::connectrpc::Spec::origin) `Client` (compare across sides with [`Spec::same_method`](::connectrpc::Spec::same_method)).
pub const GREET_SERVICE_GREET_ALL_SPEC: ::connectrpc::Spec = ::connectrpc::Spec::server(
        "/greet.v1.GreetService/GreetAll",
        ::connectrpc::StreamType::ClientStream,
    )
    .with_idempotency_level(::connectrpc::IdempotencyLevel::Unknown);
/// Static [`Spec`](::connectrpc::Spec) for the `GreetChat` RPC, as seen by the server; the generated client passes it with [`origin`](::connectrpc::Spec::origin) `Client` (compare across sides with [`Spec::same_method`](::connectrpc::Spec::same_method)).
pub const GREET_SERVICE_GREET_CHAT_SPEC: ::connectrpc::Spec = ::connectrpc::Spec::server(
        "/greet.v1.GreetService/GreetChat",
        ::connectrpc::StreamType::BidiStream,
    )
    .with_idempotency_level(::connectrpc::IdempotencyLevel::Unknown);
/// Exercises all four ConnectRPC method kinds over the Tauri IPC transport.
///
/// # Implementing handlers
///
/// Implement methods with plain `async fn`; the returned future satisfies
/// the `Send` bound automatically.
///
/// **Unary and server-streaming requests** arrive as
/// [`ServiceRequest<'_, Req>`](::connectrpc::ServiceRequest): a zero-copy
/// view of the request plus its body, valid for the duration of the call.
/// Fields are read directly (`request.name` is a `&str` into the decoded
/// buffer) and the borrow may be held across `.await` points. Anything
/// that must outlive the call — `tokio::spawn`, channels, server state,
/// or data captured by a returned response stream — takes owned data:
/// call `request.to_owned_message()` (or copy the specific fields)
/// first.
///
/// **Client-streaming and bidi requests** arrive as
/// [`InboundStream<Req>`](::connectrpc::InboundStream) — a
/// `ServiceStream` of [`StreamMessage`](::connectrpc::StreamMessage)s.
/// Each item owns its decoded buffer and is `Send + 'static`, so items
/// can be buffered or moved into spawned tasks; read fields zero-copy
/// through the generated accessor methods (`item.name()`) or `.view()`,
/// convert with `.to_owned_message()`, or yield an item back unchanged —
/// `StreamMessage<M>` implements `Encodable<M>`.
///
/// Request types resolved through `extern_path` (e.g. well-known types
/// from another crate) use the same wrappers; the crate that owns the
/// type must be generated with buffa ≥ 0.9.0 and views enabled so the
/// backing `HasMessageView` impl exists.
///
/// The `impl Encodable<Out>` return bound accepts the owned `Out`, the
/// generated `OutView<'_>` / `OwnedOutView`,
/// [`MaybeBorrowed`](::connectrpc::MaybeBorrowed), or
/// [`PreEncoded`](::connectrpc::PreEncoded) for handlers that encode a
/// non-`'static` view internally and pass the bytes across the handler
/// boundary. View bodies are not emitted for output types mapped via
/// `extern_path` (the impl would be an orphan); return owned for
/// WKT/extern outputs.
///
/// Server-streaming and bidi-streaming methods return
/// `ServiceStream<impl Encodable<Out> + Send + use<Self>>`. The
/// `use<Self>` precise-capturing clause excludes `&self`'s lifetime and
/// the request's lifetime (unary methods use `use<'a, Self>` and may
/// borrow from `&self`), so stream items must be `'static` and cannot
/// borrow from the request. To stream view-encoded data, encode each
/// item inside the stream body and yield
/// [`PreEncoded`](::connectrpc::PreEncoded) — see its `# Streaming
/// example` doc.
#[allow(clippy::type_complexity)]
pub trait GreetService: Send + Sync + 'static {
    /// Unary.
    ///
    /// `'a` lets the response body borrow from `&self` (e.g. server-resident state).
    ///
    /// `request` is borrowed from the request body and is valid for the
    /// duration of the call; message fields are read directly on it
    /// (zero-copy). The response cannot borrow from `request` — use
    /// `.to_owned_message()` (or copy the specific fields) for anything
    /// returned, stored, or moved into `tokio::spawn`.
    fn greet<'a>(
        &'a self,
        ctx: ::connectrpc::RequestContext,
        request: ::connectrpc::ServiceRequest<'_, GreetRequest>,
    ) -> impl ::std::future::Future<
        Output = ::connectrpc::ServiceResult<
            impl ::connectrpc::Encodable<GreetResponse> + Send + use<'a, Self>,
        >,
    > + Send;
    /// Server streaming: one greeting per requested count.
    ///
    /// `request` is borrowed from the request body and is valid for the
    /// duration of the call (until the response stream is returned);
    /// message fields are read directly on it (zero-copy). Data the
    /// returned stream needs must be copied out or converted via
    /// `.to_owned_message()`.
    fn greet_many(
        &self,
        ctx: ::connectrpc::RequestContext,
        request: ::connectrpc::ServiceRequest<'_, GreetManyRequest>,
    ) -> impl ::std::future::Future<
        Output = ::connectrpc::ServiceResult<
            ::connectrpc::ServiceStream<
                impl ::connectrpc::Encodable<GreetResponse> + Send + use<Self>,
            >,
        >,
    > + Send;
    /// Client streaming: collects names, responds once.
    ///
    /// `'a` lets the response body borrow from `&self` (e.g. server-resident state).
    ///
    /// Each `requests` item is a [`StreamMessage`](::connectrpc::StreamMessage):
    /// it owns its buffer, is `Send + 'static`, and exposes zero-copy
    /// accessor methods (`item.name()`), `.view()`, and
    /// `.to_owned_message()`.
    fn greet_all<'a>(
        &'a self,
        ctx: ::connectrpc::RequestContext,
        requests: ::connectrpc::InboundStream<GreetRequest>,
    ) -> impl ::std::future::Future<
        Output = ::connectrpc::ServiceResult<
            impl ::connectrpc::Encodable<GreetResponse> + Send + use<'a, Self>,
        >,
    > + Send;
    /// Bidi streaming: one response per request, interleaved.
    ///
    /// Each `requests` item is a [`StreamMessage`](::connectrpc::StreamMessage):
    /// it owns its buffer, is `Send + 'static`, and exposes zero-copy
    /// accessor methods (`item.name()`), `.view()`, and
    /// `.to_owned_message()`.
    fn greet_chat(
        &self,
        ctx: ::connectrpc::RequestContext,
        requests: ::connectrpc::InboundStream<GreetRequest>,
    ) -> impl ::std::future::Future<
        Output = ::connectrpc::ServiceResult<
            ::connectrpc::ServiceStream<
                impl ::connectrpc::Encodable<GreetResponse> + Send + use<Self>,
            >,
        >,
    > + Send;
}
/// Extension trait for registering a service implementation with a Router.
///
/// This trait is automatically implemented for all types that implement the service trait.
/// Prefer [`Router::add_service`](::connectrpc::Router::add_service) for
/// top-down registration; `register` remains available for compatibility
/// and cases where the service-first call shape is more convenient.
///
/// # Example
///
/// ```rust,ignore
/// use std::sync::Arc;
///
/// let service = Arc::new(MyServiceImpl);
/// let router = service.register(Router::new());
/// ```
pub trait GreetServiceExt: GreetService {
    /// Register this service implementation with a Router.
    ///
    /// Takes ownership of the `Arc<Self>` and returns a new Router with
    /// this service's methods registered.
    fn register(
        self: ::std::sync::Arc<Self>,
        router: ::connectrpc::Router,
    ) -> ::connectrpc::Router;
}
impl<S: GreetService> GreetServiceExt for S {
    fn register(
        self: ::std::sync::Arc<Self>,
        router: ::connectrpc::Router,
    ) -> ::connectrpc::Router {
        router
            .route_view(
                GREET_SERVICE_SERVICE_NAME,
                "Greet",
                {
                    let svc = ::std::sync::Arc::clone(&self);
                    ::connectrpc::view_handler_fn(move |
                        ctx,
                        req: ::buffa::view::OwnedView<
                            __buffa::view::GreetRequestView<'static>,
                        >,
                        format|
                    {
                        let svc = ::std::sync::Arc::clone(&svc);
                        async move {
                            let sreq = ::connectrpc::ServiceRequest::<
                                GreetRequest,
                            >::from_parts(req.reborrow(), req.bytes());
                            svc.greet(ctx, sreq).await?.encode::<GreetResponse>(format)
                        }
                    })
                },
            )
            .with_spec(GREET_SERVICE_GREET_SPEC)
            .route_view_server_stream::<
                _,
                _,
                GreetResponse,
            >(
                GREET_SERVICE_SERVICE_NAME,
                "GreetMany",
                ::connectrpc::view_streaming_handler_fn({
                    let svc = ::std::sync::Arc::clone(&self);
                    move |
                        ctx,
                        req: ::buffa::view::OwnedView<
                            __buffa::view::GreetManyRequestView<'static>,
                        >|
                    {
                        let svc = ::std::sync::Arc::clone(&svc);
                        async move {
                            let sreq = ::connectrpc::ServiceRequest::<
                                GreetManyRequest,
                            >::from_parts(req.reborrow(), req.bytes());
                            svc.greet_many(ctx, sreq).await
                        }
                    }
                }),
            )
            .with_spec(GREET_SERVICE_GREET_MANY_SPEC)
            .route_view_client_stream(
                GREET_SERVICE_SERVICE_NAME,
                "GreetAll",
                ::connectrpc::view_client_streaming_handler_fn({
                    let svc = ::std::sync::Arc::clone(&self);
                    move |ctx, req, format| {
                        let svc = ::std::sync::Arc::clone(&svc);
                        async move {
                            let req = ::connectrpc::dispatcher::codegen::into_stream_messages::<
                                GreetRequest,
                            >(req);
                            svc.greet_all(ctx, req)
                                .await?
                                .encode::<GreetResponse>(format)
                        }
                    }
                }),
            )
            .with_spec(GREET_SERVICE_GREET_ALL_SPEC)
            .route_view_bidi_stream::<
                _,
                _,
                GreetResponse,
            >(
                GREET_SERVICE_SERVICE_NAME,
                "GreetChat",
                ::connectrpc::view_bidi_streaming_handler_fn({
                    let svc = ::std::sync::Arc::clone(&self);
                    move |ctx, req| {
                        let svc = ::std::sync::Arc::clone(&svc);
                        async move {
                            let req = ::connectrpc::dispatcher::codegen::into_stream_messages::<
                                GreetRequest,
                            >(req);
                            svc.greet_chat(ctx, req).await
                        }
                    }
                }),
            )
            .with_spec(GREET_SERVICE_GREET_CHAT_SPEC)
    }
}
/// Type-inference marker used by [`Router::add_service`](::connectrpc::Router::add_service).
#[doc(hidden)]
pub struct GreetServiceRegisterMarker;
impl<S: GreetService> ::connectrpc::ServiceRegister<GreetServiceRegisterMarker>
for ::std::sync::Arc<S> {
    fn register_service(self, router: ::connectrpc::Router) -> ::connectrpc::Router {
        <S as GreetServiceExt>::register(self, router)
    }
}
/// Monomorphic dispatcher for `GreetService`.
///
/// Unlike `.register(Router)` which type-erases each method into an `Arc<dyn ErasedHandler>` stored in a `HashMap`, this struct dispatches via a compile-time `match` on method name: no vtable, no hash lookup.
///
/// # Example
///
/// ```rust,ignore
/// use connectrpc::ConnectRpcService;
///
/// let server = GreetServiceServer::new(MyImpl);
/// let service = ConnectRpcService::new(server);
/// // hand `service` to axum/hyper as a fallback_service
/// ```
pub struct GreetServiceServer<T> {
    inner: ::std::sync::Arc<T>,
}
impl<T: GreetService> GreetServiceServer<T> {
    /// Wrap a service implementation in a monomorphic dispatcher.
    pub fn new(service: T) -> Self {
        Self {
            inner: ::std::sync::Arc::new(service),
        }
    }
    /// Wrap an already-`Arc`'d service implementation.
    pub fn from_arc(inner: ::std::sync::Arc<T>) -> Self {
        Self { inner }
    }
}
impl<T> Clone for GreetServiceServer<T> {
    fn clone(&self) -> Self {
        Self {
            inner: ::std::sync::Arc::clone(&self.inner),
        }
    }
}
impl<T: GreetService> ::connectrpc::Dispatcher for GreetServiceServer<T> {
    #[inline]
    fn lookup(
        &self,
        path: &str,
    ) -> Option<::connectrpc::dispatcher::codegen::MethodDescriptor> {
        let method = path.strip_prefix("greet.v1.GreetService/")?;
        match method {
            "Greet" => {
                Some(
                    ::connectrpc::dispatcher::codegen::MethodDescriptor::unary(false)
                        .with_spec(GREET_SERVICE_GREET_SPEC),
                )
            }
            "GreetMany" => {
                Some(
                    ::connectrpc::dispatcher::codegen::MethodDescriptor::server_streaming()
                        .with_spec(GREET_SERVICE_GREET_MANY_SPEC),
                )
            }
            "GreetAll" => {
                Some(
                    ::connectrpc::dispatcher::codegen::MethodDescriptor::client_streaming()
                        .with_spec(GREET_SERVICE_GREET_ALL_SPEC),
                )
            }
            "GreetChat" => {
                Some(
                    ::connectrpc::dispatcher::codegen::MethodDescriptor::bidi_streaming()
                        .with_spec(GREET_SERVICE_GREET_CHAT_SPEC),
                )
            }
            _ => None,
        }
    }
    fn call_unary(
        &self,
        path: &str,
        ctx: ::connectrpc::RequestContext,
        request: ::connectrpc::Payload,
        format: ::connectrpc::CodecFormat,
    ) -> ::connectrpc::dispatcher::codegen::UnaryResult {
        let Some(method) = path.strip_prefix("greet.v1.GreetService/") else {
            return ::connectrpc::dispatcher::codegen::unimplemented_unary(path);
        };
        let _ = (&ctx, &request, &format);
        match method {
            "Greet" => {
                let svc = ::std::sync::Arc::clone(&self.inner);
                Box::pin(async move {
                    let body = ::connectrpc::dispatcher::codegen::request_proto_bytes::<
                        GreetRequest,
                    >(request.encoded()?, format)?;
                    let req: __buffa::view::GreetRequestView<'_> = ::connectrpc::dispatcher::codegen::decode_borrowed_request_view(
                        &body,
                        ctx.decode_options(),
                    )?;
                    let req = ::connectrpc::ServiceRequest::<
                        GreetRequest,
                    >::from_parts(&req, &body);
                    svc.greet(ctx, req).await?.encode::<GreetResponse>(format)
                })
            }
            _ => ::connectrpc::dispatcher::codegen::unimplemented_unary(path),
        }
    }
    fn call_server_streaming(
        &self,
        path: &str,
        ctx: ::connectrpc::RequestContext,
        request: ::buffa::bytes::Bytes,
        format: ::connectrpc::CodecFormat,
    ) -> ::connectrpc::dispatcher::codegen::StreamingResult {
        let Some(method) = path.strip_prefix("greet.v1.GreetService/") else {
            return ::connectrpc::dispatcher::codegen::unimplemented_streaming(path);
        };
        let _ = (&ctx, &request, &format);
        match method {
            "GreetMany" => {
                let svc = ::std::sync::Arc::clone(&self.inner);
                Box::pin(async move {
                    let body = ::connectrpc::dispatcher::codegen::request_proto_bytes::<
                        GreetManyRequest,
                    >(request, format)?;
                    let req: __buffa::view::GreetManyRequestView<'_> = ::connectrpc::dispatcher::codegen::decode_borrowed_request_view(
                        &body,
                        ctx.decode_options(),
                    )?;
                    let req = ::connectrpc::ServiceRequest::<
                        GreetManyRequest,
                    >::from_parts(&req, &body);
                    let resp = svc.greet_many(ctx, req).await?;
                    Ok(
                        resp
                            .map_body(|s| ::connectrpc::dispatcher::codegen::encode_response_stream::<
                                GreetResponse,
                                _,
                                _,
                            >(s, format)),
                    )
                })
            }
            _ => ::connectrpc::dispatcher::codegen::unimplemented_streaming(path),
        }
    }
    fn call_client_streaming(
        &self,
        path: &str,
        ctx: ::connectrpc::RequestContext,
        requests: ::connectrpc::dispatcher::codegen::RequestStream,
        format: ::connectrpc::CodecFormat,
    ) -> ::connectrpc::dispatcher::codegen::UnaryResult {
        let Some(method) = path.strip_prefix("greet.v1.GreetService/") else {
            return ::connectrpc::dispatcher::codegen::unimplemented_unary(path);
        };
        let _ = (&ctx, &requests, &format);
        match method {
            "GreetAll" => {
                let svc = ::std::sync::Arc::clone(&self.inner);
                Box::pin(async move {
                    let req_stream = ::connectrpc::dispatcher::codegen::decode_message_request_stream::<
                        GreetRequest,
                    >(requests, format, ctx.decode_options().clone());
                    svc.greet_all(ctx, req_stream).await?.encode::<GreetResponse>(format)
                })
            }
            _ => ::connectrpc::dispatcher::codegen::unimplemented_unary(path),
        }
    }
    fn call_bidi_streaming(
        &self,
        path: &str,
        ctx: ::connectrpc::RequestContext,
        requests: ::connectrpc::dispatcher::codegen::RequestStream,
        format: ::connectrpc::CodecFormat,
    ) -> ::connectrpc::dispatcher::codegen::StreamingResult {
        let Some(method) = path.strip_prefix("greet.v1.GreetService/") else {
            return ::connectrpc::dispatcher::codegen::unimplemented_streaming(path);
        };
        let _ = (&ctx, &requests, &format);
        match method {
            "GreetChat" => {
                let svc = ::std::sync::Arc::clone(&self.inner);
                Box::pin(async move {
                    let req_stream = ::connectrpc::dispatcher::codegen::decode_message_request_stream::<
                        GreetRequest,
                    >(requests, format, ctx.decode_options().clone());
                    let resp = svc.greet_chat(ctx, req_stream).await?;
                    Ok(
                        resp
                            .map_body(|s| ::connectrpc::dispatcher::codegen::encode_response_stream::<
                                GreetResponse,
                                _,
                                _,
                            >(s, format)),
                    )
                })
            }
            _ => ::connectrpc::dispatcher::codegen::unimplemented_streaming(path),
        }
    }
}
/// Client for this service.
///
/// Generic over `T: ClientTransport`. For **gRPC** (HTTP/2), use
/// `Http2Connection` — it has honest `poll_ready` and composes with
/// `tower::balance` for multi-connection load balancing. For **Connect
/// over HTTP/1.1** (or unknown protocol), use `HttpClient`.
///
/// # Example (gRPC / HTTP/2)
///
/// ```rust,ignore
/// use connectrpc::client::{Http2Connection, ClientConfig};
/// use connectrpc::Protocol;
///
/// let uri: http::Uri = "http://localhost:8080".parse()?;
/// let conn = Http2Connection::connect_plaintext(uri.clone()).await?.shared(1024);
/// let config = ClientConfig::new(uri).with_protocol(Protocol::Grpc);
///
/// let client = GreetServiceClient::new(conn, config);
/// let response = client.greet(request).await?;
/// ```
///
/// # Example (Connect / HTTP/1.1 or ALPN)
///
/// ```rust,ignore
/// use connectrpc::client::{HttpClient, ClientConfig};
///
/// let http = HttpClient::plaintext();  // cleartext http:// only
/// let config = ClientConfig::new("http://localhost:8080".parse()?);
///
/// let client = GreetServiceClient::new(http, config);
/// let response = client.greet(request).await?;
/// ```
///
/// # Working with the response
///
/// Unary calls return [`UnaryResponse<OwnedView<FooView>>`](::connectrpc::client::UnaryResponse).
/// [`view()`](::connectrpc::client::UnaryResponse::view) borrows the response
/// message, so field access is zero-copy:
///
/// ```rust,ignore
/// let resp = client.greet(request).await?;
/// let name: &str = resp.view().name;  // borrow into the response buffer
/// ```
///
/// If you need the owned struct (e.g. to store or pass by value), use
/// [`into_owned()`](::connectrpc::client::UnaryResponse::into_owned):
///
/// ```rust,ignore
/// let owned = client.greet(request).await?.into_owned();
/// ```
///
/// [`into_view()`](::connectrpc::client::UnaryResponse::into_view) keeps the
/// zero-copy decoded body (an `OwnedView`) without copying; field access on it
/// goes through `.reborrow()`. Streaming responses yield one
/// [`StreamMessage`](::connectrpc::StreamMessage) per received message from
/// `.message().await` — read fields zero-copy through the generated accessor
/// methods (`msg.name()`) or `.view()`, or convert with `.to_owned_message()`.
#[derive(Clone)]
pub struct GreetServiceClient<T> {
    transport: T,
    config: ::connectrpc::client::ClientConfig,
}
impl<T> GreetServiceClient<T>
where
    T: ::connectrpc::client::ClientTransport,
    <T::ResponseBody as ::connectrpc::http_body::Body>::Error: ::std::fmt::Display,
{
    /// Create a new client with the given transport and configuration.
    pub fn new(transport: T, config: ::connectrpc::client::ClientConfig) -> Self {
        Self { transport, config }
    }
    /// Get the client configuration.
    pub fn config(&self) -> &::connectrpc::client::ClientConfig {
        &self.config
    }
    /// Get a mutable reference to the client configuration.
    pub fn config_mut(&mut self) -> &mut ::connectrpc::client::ClientConfig {
        &mut self.config
    }
    /// Call the Greet RPC. Sends a request to /greet.v1.GreetService/Greet.
    pub async fn greet(
        &self,
        request: GreetRequest,
    ) -> Result<
        ::connectrpc::client::UnaryResponse<
            ::buffa::view::OwnedView<__buffa::view::GreetResponseView<'static>>,
        >,
        ::connectrpc::ConnectError,
    > {
        self.greet_with_options(request, ::connectrpc::client::CallOptions::default())
            .await
    }
    /// Call the Greet RPC with explicit per-call options. Options override [`ClientConfig`](::connectrpc::client::ClientConfig) defaults.
    pub async fn greet_with_options(
        &self,
        request: GreetRequest,
        options: ::connectrpc::client::CallOptions,
    ) -> Result<
        ::connectrpc::client::UnaryResponse<
            ::buffa::view::OwnedView<__buffa::view::GreetResponseView<'static>>,
        >,
        ::connectrpc::ConnectError,
    > {
        ::connectrpc::client::call_unary(
                &self.transport,
                &self.config,
                GREET_SERVICE_GREET_SPEC.with_origin(::connectrpc::SpecOrigin::Client),
                request,
                options,
            )
            .await
    }
    /// Call the GreetMany RPC. Sends a request to /greet.v1.GreetService/GreetMany.
    pub async fn greet_many(
        &self,
        request: GreetManyRequest,
    ) -> Result<
        ::connectrpc::client::ServerStream<
            T::ResponseBody,
            __buffa::view::GreetResponseView<'static>,
        >,
        ::connectrpc::ConnectError,
    > {
        self.greet_many_with_options(
                request,
                ::connectrpc::client::CallOptions::default(),
            )
            .await
    }
    /// Call the GreetMany RPC with explicit per-call options. Options override [`ClientConfig`](::connectrpc::client::ClientConfig) defaults.
    pub async fn greet_many_with_options(
        &self,
        request: GreetManyRequest,
        options: ::connectrpc::client::CallOptions,
    ) -> Result<
        ::connectrpc::client::ServerStream<
            T::ResponseBody,
            __buffa::view::GreetResponseView<'static>,
        >,
        ::connectrpc::ConnectError,
    > {
        ::connectrpc::client::call_server_stream(
                &self.transport,
                &self.config,
                GREET_SERVICE_GREET_MANY_SPEC
                    .with_origin(::connectrpc::SpecOrigin::Client),
                request,
                options,
            )
            .await
    }
    /// Call the GreetAll RPC. Sends a request to /greet.v1.GreetService/GreetAll.
    ///
    /// `requests` is any `Stream<Item = ...> + Send + 'static` of
    /// request messages (the `ClientRequestStream` bound); messages
    /// are sent as the stream yields them. It backs the request
    /// body, so yield owned messages or feed the call from a
    /// channel-backed stream. For a collection that is already in
    /// hand, wrap it with `::connectrpc::stream_iter(...)`.
    ///
    /// Dropping the returned future cancels the call: the request
    /// body is dropped along with it, so messages the stream had
    /// not yet yielded are never delivered. A caller that needs the
    /// request delivered must drive the call to completion rather
    /// than, say, wrapping it in a `timeout`.
    pub async fn greet_all(
        &self,
        requests: impl ::connectrpc::client::ClientRequestStream<GreetRequest>,
    ) -> Result<
        ::connectrpc::client::UnaryResponse<
            ::buffa::view::OwnedView<__buffa::view::GreetResponseView<'static>>,
        >,
        ::connectrpc::ConnectError,
    > {
        self.greet_all_with_options(
                requests,
                ::connectrpc::client::CallOptions::default(),
            )
            .await
    }
    /// Call the GreetAll RPC with explicit per-call options. Options override [`ClientConfig`](::connectrpc::client::ClientConfig) defaults.
    ///
    /// `requests` is any `Stream<Item = ...> + Send + 'static` of
    /// request messages (the `ClientRequestStream` bound); messages
    /// are sent as the stream yields them. It backs the request
    /// body, so yield owned messages or feed the call from a
    /// channel-backed stream. For a collection that is already in
    /// hand, wrap it with `::connectrpc::stream_iter(...)`.
    ///
    /// Dropping the returned future cancels the call: the request
    /// body is dropped along with it, so messages the stream had
    /// not yet yielded are never delivered. A caller that needs the
    /// request delivered must drive the call to completion rather
    /// than, say, wrapping it in a `timeout`.
    pub async fn greet_all_with_options(
        &self,
        requests: impl ::connectrpc::client::ClientRequestStream<GreetRequest>,
        options: ::connectrpc::client::CallOptions,
    ) -> Result<
        ::connectrpc::client::UnaryResponse<
            ::buffa::view::OwnedView<__buffa::view::GreetResponseView<'static>>,
        >,
        ::connectrpc::ConnectError,
    > {
        ::connectrpc::client::call_client_stream(
                &self.transport,
                &self.config,
                GREET_SERVICE_GREET_ALL_SPEC
                    .with_origin(::connectrpc::SpecOrigin::Client),
                requests,
                options,
            )
            .await
    }
    /// Call the GreetChat RPC. Sends a request to /greet.v1.GreetService/GreetChat.
    pub async fn greet_chat(
        &self,
    ) -> Result<
        ::connectrpc::client::BidiStream<
            T::ResponseBody,
            GreetRequest,
            __buffa::view::GreetResponseView<'static>,
        >,
        ::connectrpc::ConnectError,
    > {
        self.greet_chat_with_options(::connectrpc::client::CallOptions::default()).await
    }
    /// Call the GreetChat RPC with explicit per-call options. Options override [`ClientConfig`](::connectrpc::client::ClientConfig) defaults.
    pub async fn greet_chat_with_options(
        &self,
        options: ::connectrpc::client::CallOptions,
    ) -> Result<
        ::connectrpc::client::BidiStream<
            T::ResponseBody,
            GreetRequest,
            __buffa::view::GreetResponseView<'static>,
        >,
        ::connectrpc::ConnectError,
    > {
        ::connectrpc::client::call_bidi_stream(
                &self.transport,
                &self.config,
                GREET_SERVICE_GREET_CHAT_SPEC
                    .with_origin(::connectrpc::SpecOrigin::Client),
                options,
            )
            .await
    }
}
