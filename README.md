# simplegram
A simple distributed messenger app.

Authors; [Georgios E. Syros](https://github.com/gsiros "Georgios E. Syros"), [Anastasios Toumazatos](https://github.com/toumazatos "Anastasios Toumazatos"), [Nikos Christodoulou](https://github.com/nikos-christodoulou "Nikos Christodoulou"), [Evgenios Gkritsis](https://github.com/eGkritsis "Evgenios Gkritsis")

## Introdction

The objective was to develop a communication system that supports multimedia content in Java. Text, photo or video content is published by one or more users and is delivered to a set of one or more subscribers. Due to the large amount of users that we want to serve, we implemented a clever system that is capable of content delivery to the correct receivers. 

In order to distribute the content, we need to know: 
- **who is intersted** (_subscribers_)
- **how how can they express interest** (_topic subscription_) and
- **how can they receive it**.

## Event Delivery System

This repoisotory accomodates the implementation of the multimedia streaming framework (_Event Delivery System_) which is responsible to support the forwarding and receiving (_streaming_) of multimedia conent. 

The Event Delivery System is a programming framework that allows sending and receiving data that fulfil specific criteria. The advantage of the Event Delivery System is the immediate forwarding of data in real time via two fundamental functions; `push` and `pull`. These two functions are independent of each other. 

During each `push` call, the intermediate system node (_broker_) should;
- **be able to handle data incoming from different _publishers_ concurrently** (in our case users) and
- **be able to deliver the results to the _subscribers_** (also called _consumers_ as they "consume" the data)

Concurrency is inevitable because the system is required to offer simultaneous data delivery from _publishers_ to _brokers_ and from _brokers_ to _subscribers_. All subscribed users must simultaneously receive the same content.

The aforementioned model is backed backed by the two core functions that can be described by the following figure;

<p align="center">
  <img src="https://user-images.githubusercontent.com/47118034/177747351-27c07f22-1789-4329-9f4a-5126231dce26.png" />
</p>

<p align="center">
  <i>Figure 1. A basic prototype of the system.</i> 
</p>

A brief description follows.

### - `push`

The sole role of the `push` function is to forward to a _broker_ a value which is stored in a data structure (e.g. queue), so that the value can be delivered upon requested. This intermediate data structure plays the role of the topic's _chat history_. As a result, once a new user subscribes to the topic, they will be able to see the previous messages. In our case, `push` takes as input the information required for the immaculate delievery of the content (ex. _username, topic name/id, video data, etc._). 

A significant software requirement that needs to be addressed for better comprehension of the model's functionality is **multimedia chunking**; a photo or a video streamed to and from the framework is *never sent wholly*. On the contrary, multimedia content is cut down to smaller, equal in size, fragments (_chunks_) in order to achieve higher communication efficiency[^1].

### - `pull`

The role of `pull` is to deliver all the data of an intermediate node that concern the user (_subscriber_) that calls the function. Values from each topic are collected and delivered to the _subscriber_ that issued the request. 

[^1]: This mechanism might be **redundant** due to the usage of TCP, which handles data fragmentation on its own, for nearly all communication purposes. 
