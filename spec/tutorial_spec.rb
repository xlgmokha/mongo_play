require "spec_helper"

describe "mongo" do
  let(:connection) { Mongo::MongoClient.new("localhost", 27017) }
  let(:db) { connection.db("mydb") }

  after :each do
    db['test'].remove()
  end

  after :all do
    connection.drop_database("mydb")
  end

  it "lists all the databases" do
    connection.database_names.should_not be_empty
    connection.database_info.each { |x| puts x.inspect }
  end

  it "connects to a database" do
    db.should_not be_nil
  end

  it "connects to a collection" do
    test = db.collection("test")
    test.should_not be_nil
  end

  it "can insert into a collection" do
    document = { "name" => "MongoDB", "type" => "database", "count" => 1, "info" => { "x" => 203, "y" => "102" } }
    collection = db['test']
    id = collection.insert(document)
    id.should_not be_nil
    collection.find_one.should_not be_nil
    collection.find("_id" => id).first['name'].should == 'MongoDB'
  end

  it "can list all collections" do
    db.collection_names.should_not be_empty
  end

  it "can insert multiple documents" do
    collection = db['test']
    100.times { |x| collection.insert("i" => x) }
    collection.find.count.should == 100
  end

  it "can sort" do
    collection = db['test']
    100.times { |x| collection.insert("i" => x) }
    results = collection.find.sort(:i).to_a
    100.times { |x| results[x]['i'].should == x }
    results = collection.find.sort(:i => :desc).to_a.reverse
    100.times { |x| results[x]['i'].should == x }
  end

  it "can count" do
    collection = db['test']
    100.times { |x| collection.insert("i" => x) }
    collection.count.should == 100
  end

  it "can find items greater than 50" do
    collection = db['test']
    100.times { |x| collection.insert("i" => x) }
    results = collection.find("i" => { "$gt" => 50 }).map {|x| x['i'] }.to_a
    results.count.should == 49
    (51...100).each { |x| results.should include(x) }
  end

  it "can find items between 20 and 30" do
    collection = db['test']
    100.times { |x| collection.insert("i" => x) }
    results = collection.find("i" => { "$gte" => 20, "$lte" => 30}).map {|x| x['i'] }.to_a
    results.count.should == 11
    (20...30).each { |x| results.should include(x) }
  end

  it "can return specific fields" do
    collection = db['test']
    id = collection.insert({'name' => 'blah', 'type' => "MongoDb"})
    result = collection.find({"_id" => id}, :fields => ["name"]).first
    result['name'].should == 'blah'
  end

  it "can also find using a regex" do
    collection = db['test']
    collection.insert({'name' => 'blah', 'type' => "MongoDb"})
    result = collection.find({"name" => /b/}).first
    result['name'].should == 'blah'
  end

  it "can update a document" do
    collection = db['test']
    id = collection.insert({"name" => 'mo'})
    collection.update({"_id" => id}, {"name" => 'om'})
    result = collection.find("_id" => id).first
    result['name'].should == 'om'
  end

  it "can update a document using an atomic operator" do
    collection = db['test']
    id = collection.insert({"name" => 'mo'})
    collection.update({"_id" => id}, { "$set" => { "name" => 'om'}})
    result = collection.find("_id" => id).first
    result['name'].should == 'om'
  end

  it "can remove documents" do
    collection = db['test']
    100.times { |x| collection.insert("i" => x) }
    collection.remove("i" => 71)
    collection.count.should == 99
    collection.find("i" => 71).to_a.should be_empty
  end

  it "can remove all documents" do
    collection = db['test']
    100.times { |x| collection.insert("i" => x) }
    collection.remove
    collection.count.should == 0
  end

  it "can explain the query" do
    collection = db['test']
    100.times { |x| collection.insert("i" => x) }
    collection.find("i" => 71).explain.should_not be_nil
  end

  it "can provide index information" do
    collection = db['test']
    collection.index_information.should_not be_nil
  end

  it "can drop a collection" do
    collection = db['test']
    db.collection_names.should include 'test'
    collection.drop
    db.collection_names.should_not include 'test'
  end

  it "can drop a database" do
    connection.database_names.should include('mydb')
    connection.drop_database("mydb")
    connection.database_names.should_not include('mydb')
  end
end
