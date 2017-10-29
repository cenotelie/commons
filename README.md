# Cénotélie Commons #

Cénotélie Commons is a set of useful Java API that can be leveraged for other projects.
Commons provides general utility APIs and implementation of other specifications, most notably:

* Implementation of [JSON-RPC](http://www.jsonrpc.org/specification)
* Implementation of [Language Server Protocol](https://langserver.org/)


## How do I use this software? ##

Cénotélie Commons bundle are intended to be primarily used as libraries in other projects.
You can add them as dependencies into your project.
For example, the implementation of the [Language Server Protocol](https://langserver.org/) can be added as a Maven dependency with:

```
<dependency>
    <groupId>fr.cenotelie.commons</groupId>
    <artifactId>commons-lsp</artifactId>
    <version>1.0.0</version>
    <scope>compile</scope>
</dependency>
```


## Repository structure ##

* `utils`: General utility APIs, including the JSON parser.
* `jsonrpc`: Implementation of [JSON-RPC](http://www.jsonrpc.org/specification)
* `lsp`: Implementation of [Language Server Protocol](https://langserver.org/)


## How to build ##

To build the artifacts in this repository using Maven:

```
$ mvn clean install -Dgpg.skip=true
```


## How can I contribute? ##

The simplest way to contribute is to:

* Fork this repository on [Bitbucket](https://bitbucket.org/cenotelie/commons).
* Fix [some issue](https://bitbucket.org/cenotelie/commons/issues?status=new&status=open) or implement a new feature.
* Create a pull request on Bitbucket.

Patches can also be submitted by email, or through the [issue management system](https://bitbucket.org/cenotelie/commons/issues).

The [isse tracker](https://bitbucket.org/cenotelie/commons/issues) may contain tickets that are accessible to newcomers. Look for tickets with `[beginner]` in the title. These tickets are good ways to become more familiar with the project and the codebase.


## License ##

This software is licenced under the Lesser General Public License (LGPL) v3.
Refers to the `LICENSE.txt` file at the root of the repository for the full text, or to [the online version](http://www.gnu.org/licenses/lgpl-3.0.html).
