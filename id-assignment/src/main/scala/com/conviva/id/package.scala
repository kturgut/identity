package com.conviva

import com.conviva.id.schema.PublicIpType

package object id {

  val SchemaVersion = "0.0.1"

  type Ip = PublicIpType

  type ClientId = Seq[Int]

}

