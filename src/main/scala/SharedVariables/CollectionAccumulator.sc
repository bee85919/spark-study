val listAcc= sc.collectionAccumulator[List[String]]("list accum")
listAcc.add(List("Paul", "Nancy"))
listAcc.add(List("Paul", "Cathy"))
listAcc.value