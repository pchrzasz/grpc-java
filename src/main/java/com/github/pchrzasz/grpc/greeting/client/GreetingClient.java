package com.github.pchrzasz.grpc.greeting.client;

import com.proto.greet.*;
import com.proto.greet.GreetServiceGrpc.GreetServiceBlockingStub;
import com.proto.greet.GreetServiceGrpc.GreetServiceStub;
import io.grpc.*;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.StreamObserver;

import javax.net.ssl.SSLException;
import java.io.File;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * @author pchrzasz
 */
class GreetingClient {

	ManagedChannel channel;

	public static void main(String[] args) throws SSLException {
		System.out.println("Hello I'm a gRPC client");

		GreetingClient main = new GreetingClient();
		main.run();
	}

	public void run() throws SSLException {
//		channel = ManagedChannelBuilder.forAddress("localhost", 50051)
//				.usePlaintext()
//				.build();

		// with server authentication
		channel = NettyChannelBuilder.forAddress("localhost", 50051)
				.sslContext(GrpcSslContexts.forClient().trustManager(new File("ssl/ca.crt")).build())
				.build();

		invokeUnaryCall(channel);
//		invokeServerStreamingCall(channel);
//		invokeClientStreamingCall(channel);
//		invokeBiDiStreamingCall(channel);
//		invokeUnaryCallWithDeadline(channel);

		System.out.println("Shutting down channel");
		channel.shutdown();
	}


	private void invokeUnaryCall(ManagedChannel channel) {
		// created a greet service client (blocking - synchronous)
		GreetServiceBlockingStub greetClient = GreetServiceGrpc.newBlockingStub(channel);

		// created a protocol buffer greeting message
		Greeting greeting = Greeting.newBuilder().setFirstName("Pawel").setLastName("Cha").build();

		// do the same for GreetRequest
		GreetRequest request = GreetRequest.newBuilder().setGreeting(greeting).build();

		// call the RPC and get back a GreetResponse (protocol buffers)
		GreetResponse response = greetClient.greet(request);

		System.out.println(response.getResult());
	}

	private void invokeServerStreamingCall(ManagedChannel channel) {
		// created a greet service client (blocking - synchronous)
		GreetServiceBlockingStub greetClient = GreetServiceGrpc.newBlockingStub(channel);

		Greeting greeting = Greeting.newBuilder().setFirstName("Pawel").build();

		GreetManyTimesRequest request = GreetManyTimesRequest.newBuilder()
				.setGreeting(greeting).build();

		greetClient.greetManyTimes(request).forEachRemaining(greetManyTimesResponse -> {
			System.out.println(greetManyTimesResponse.getResult());
		});
	}

	private void invokeClientStreamingCall(ManagedChannel channel) {
		GreetServiceStub asyncClient = GreetServiceGrpc.newStub(channel);

		CountDownLatch latch = new CountDownLatch(1);

		StreamObserver<LongGreetRequest> requestObserver = asyncClient.longGreet(new StreamObserver<LongGreetResponse>() {
			@Override
			public void onNext(LongGreetResponse longGreetResponse) {
				// we get a response from the server (only once)
				System.out.println("Received response from the server");
				System.out.println(longGreetResponse.getResult());
			}

			@Override
			public void onError(Throwable throwable) {
				// we get an error from the server
			}

			@Override
			public void onCompleted() {
				// the server is done sending data
				// will be called right after onNext()
				System.out.println("Server completed sending sth");
				latch.countDown();
			}
		});

		// streaming message #1
		System.out.println("Sending message 1");
		Greeting greeting = Greeting.newBuilder()
				.setFirstName("Pawel")
				.build();
		requestObserver.onNext(LongGreetRequest.newBuilder().setGreeting(greeting).build());

		// streaming message #2
		System.out.println("Sending message 2");
		greeting = Greeting.newBuilder()
				.setFirstName("Andrzej")
				.build();
		requestObserver.onNext(LongGreetRequest.newBuilder().setGreeting(greeting).build());

		// streaming message #3
		System.out.println("Sending message 3");
		greeting = Greeting.newBuilder()
				.setFirstName("Darek")
				.build();
		requestObserver.onNext(LongGreetRequest.newBuilder().setGreeting(greeting).build());

		// we tell the server that the client is done sending data
		requestObserver.onCompleted();

		try {
			latch.await(3L, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	private void invokeBiDiStreamingCall(ManagedChannel channel) {
		GreetServiceStub asyncClient = GreetServiceGrpc.newStub(channel);

		CountDownLatch latch = new CountDownLatch(1);

		StreamObserver<GreetEveryoneRequest> requestObserver = asyncClient.greetEveryone(new StreamObserver<GreetEveryoneResponse>() {
			@Override
			public void onNext(GreetEveryoneResponse greetEveryoneResponse) {
				System.out.println("Response from server: " + greetEveryoneResponse.getResult());
			}

			@Override
			public void onError(Throwable throwable) {
				latch.countDown();
			}

			@Override
			public void onCompleted() {
				System.out.println("Server is done sending data");
				latch.countDown();
			}
		});

		Arrays.asList("Pawel", "Darek", "Andrzej").forEach(
				name -> {
					System.out.println("Sending " + name);
					requestObserver.onNext(GreetEveryoneRequest.newBuilder()
							.setGreeting(Greeting.newBuilder()
									.setFirstName(name)
									.build())
							.build());
					try {
						Thread.sleep(200);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
		);

		requestObserver.onCompleted();

		try {
			latch.await(3, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

	}

	private void invokeUnaryCallWithDeadline(ManagedChannel channel) {
		GreetServiceBlockingStub blockingStub = GreetServiceGrpc.newBlockingStub(channel);

		invokeDeadlineWithDuration(blockingStub, 3000);
		invokeDeadlineWithDuration(blockingStub, 100);
	}

	private void invokeDeadlineWithDuration(GreetServiceBlockingStub blockingStub, long duration) {
		try {
			System.out.println("Sending a request with a deadline of " + duration + " ms");
			// first call (500 ms deadline)
			GreetWithDeadlineResponse response = blockingStub.withDeadline(Deadline.after(duration, TimeUnit.MILLISECONDS))
					.greetWithDeadline(
							GreetWithDeadlineRequest.newBuilder().setGreeting(
									Greeting.newBuilder().setFirstName("Pawel")
							).build());
			System.out.println(response.getResult());
		} catch (StatusRuntimeException e) {
			if (e.getStatus() == Status.DEADLINE_EXCEEDED) {
				System.out.println("Deadline has been exceeded, we do not want the response");
			} else {
				e.printStackTrace();
			}
		}
	}
}
