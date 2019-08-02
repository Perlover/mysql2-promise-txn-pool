/*
* This is the module originally created for Perl: DBIx::TxnPool
* Now it was not tested yet
* Many examples and description please to see here: https://metacpan.org/pod/DBIx::TxnPool
* The documentation will be created later
* Author: Perlover
* */
const wait = require('util').promisify(setTimeout)

module.exports = class TxnPool {
    constructor({size = 100, maxRepeatedDeadlocks = 5, connection}) {
        this.size = 100
        this.maxRepeatedDeadlocks = maxRepeatedDeadlocks
        this.pool = []
        this.amountDeadlocks = 0
        this.repeatedDeadlocks = 0
        this.connection = connection
        this.sortCallback = null
        this.postCallback = null
        this.commitCallback = null
        this.inTxn = false
    }

    item(cb) {
        this.itemCallback = cb
        return this
    }

    post(cb) {
        this.postCallback = cb
        return this
    }

    commit(cb) {
        this.commitCallback = cb
        return this
    }

    sort(cb) {
        this.sortCallback = cb
        return this
    }

    async add(item) {
        try {
            this.pool.push(item)
            if (! this.sortCallback) {
                await this.startTxn()
                await this.itemCallback.call(this, item)
            }
        }
        catch (e) {
            await this._checkDeadlock(e)
        }

        if (this.pool.length >= this.size)
            this.finish()
    }

    async playPool() {
        await this.startTxn()

        await wait(++this.repeatedDeadlocks * 500)

        try {
            for (let item of this.pool) {
                await this.itemCallback.call(this, item)
            }
        }
        catch (e) {
            await this._checkDeadlock(e)
        }
    }

    async finish() {
        if (this.sortCallback && this.pool.length) {
            this.pool = this.pool.sort(this.sortCallback)
            await this.playPool()
        }

        await this.commitTxn()

        if (this.postCallback) {
            for (let item of this.pool) {
                await this.postCallback.call(this, item)
            }
        }

        this.pool = []
    }

    async _checkDeadlock(error) {
        this.rollbackTxn()

        if (error.errno && (error.errno === 1213 || error.errno === 1205)) {
            this.amountDeadlocks++
            if (this.repeatedDeadlocks >= this.maxRepeatedDeadlocks) {
                this.pool = []
                this.throwError(`The deadlock limit was reached (${this.repeatedDeadlocks})`)
            }
            else {
                await this.playPool()
            }
        }
        else {
            this.pool = []
            this.throwError(`The error in item callback ${error.message}`)
        }
    }

    async startTxn() {
        if (! this.inTxn) {
            await this.connection.beginTransaction()
            this.inTxn = true
        }

    }

    async commitTxn() {
        if (this.inTxn) {
            try {
                await this.connection.commit()
                this.inTxn = false
            }
            catch(e) {
                await this._checkDeadlock(e)
                await this.commitTxn()
                return
            }

            this.repeatedDeadlocks = 0
            if (this.commitCallback)
                await this.commitCallback.call(this)
        }
    }


    async rollbackTxn() {
        if (this.inTxn) {
            await this.connection.rollback()
            this.inTxn = false
        }
    }

    throwError(str) {
        throw new Error(str)
    }
}
