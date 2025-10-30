import{d as s,b as e,f as t,c as a,A as r}from"../../../../../nitro/nitro.mjs";const o=s(async s=>{e(s,"owner"),e(s,"repo");const o=t(s);if("git-upload-pack"!=o.service&&"git-receive-pack"!=o.service)throw a({statusCode:400,statusMessage:"Not supported service"});if("git-upload-pack"==o.service)return r(s)});export{o as default};
//# sourceMappingURL=refs.get.mjs.map
