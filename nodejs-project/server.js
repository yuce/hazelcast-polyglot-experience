const { Client } = require("hazelcast-client");

class Person {
    static get CLASS_ID() {
        return 1;
    }

    constructor(name, age) {
        this.name = name;
        this.age = age;
    }

}


class PersonIdentified {
    static get FACTORY_ID() {
        return 1;
    }

    static factory(classId) {
        if (classId === Person.CLASS_ID) {
            return new PersonIdentified();
        }
        return null;
    }

    constructor(person) {
        this.person = person;
        this.factoryId = PersonIdentified.FACTORY_ID;
        this.classId = Person.CLASS_ID;
    }

    writeData(output) {
        output.writeUTF(this.person.name);
        output.writeInt(this.person.age);
    }

    readData(input) {
        const name = input.readUTF();
        const age = input.readInt();
        this.person = new Person(name, age);
    }

    toString() {
        return `PersonIdentified(${this.person.name}, ${this.person.age})`;
    }
}

class PersonPortable {
    static get FACTORY_ID() {
        return 1;
    }

    static factory(classId) {
        if (classId === PersonPortable.FACTORY_ID) {
            return new PersonPortable();
        }
        return null;
    }

    constructor(person) {
        this.person = person;
        this.factoryId = PersonPortable.FACTORY_ID;
        this.classId = Person.CLASS_ID;
    }

    writePortable(writer) {
        writer.writeUTF(this.person.name);
        writer.writeInt(this.person.age);
    }

    readPortable(reader) {
        const name = reader.readUTF("name");
        const age = reader.readInt("age");
        this.person = new Person(name, age);
    }

    toString() {
        return `PersonPortable(${this.person.name}, ${this.person.age})`;
    }
}

async function main(client) {
    const polyglotMap = await client.getMap("polyglot");

    const pythonInteger = await polyglotMap.get("python-integer")
    console.log(`pythonInteger: ${pythonInteger}`);

    const pythonFloat = await polyglotMap.get("python-float");
    console.log(`pythonFloat: ${pythonFloat}`);

    const pythonBool = await polyglotMap.get("python-bool");
    console.log(`pythonBool: ${pythonBool}`);

    const pythonList = await polyglotMap.get("python-list");
    console.log(`pythonList: ${pythonList}`);

    const pythonString = await polyglotMap.get("python-string");
    console.log(`pythonString: ${pythonString}`);

    const pythonIdentified = await polyglotMap.get("python-identified");
    console.log(`pythonIdentified: ${pythonIdentified}`);

    const pythonPortable = await polyglotMap.get("python-portable");
    console.log(`pythonPortable: ${pythonPortable}`);

    const javaArray = await polyglotMap.get("java-array");
    console.log(`javaArray: ${javaArray}`);

    await polyglotMap.put("nodejs-array", [1, 2, 3]);

    return await client.shutdown();
}

function makeConfig() {
    const config = {
        serialization: {
            dataSerializableFactories: {},
            portableFactories: {}
        }
    }
    config.serialization.dataSerializableFactories[PersonIdentified.FACTORY_ID] =
        PersonIdentified.factory;
    config.serialization.portableFactories[PersonPortable.FACTORY_ID] =
        PersonPortable.factory;
    return config;
}

(async () => {
    let client = null;
    try {
        client = await Client.newHazelcastClient(makeConfig());
        await main(client);
   } catch (e) {
       console.error("Error running main", e);
        try {
            if (client) await client.shutdown();
        } catch (e) {
            // pass
        }
   }
})();
