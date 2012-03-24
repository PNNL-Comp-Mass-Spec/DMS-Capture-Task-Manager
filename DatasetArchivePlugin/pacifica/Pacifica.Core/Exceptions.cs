using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Pacifica.Core
{
    public class RequiredInfoNotProvidedException : Exception
    {
        private string[] _requiredFieldsNotProvided;

        public RequiredInfoNotProvidedException(string[] requiredFieldsNotProvided)
        {
            _requiredFieldsNotProvided = requiredFieldsNotProvided;
        }

        public override string Message
        {
            get
            {
                string msg = "The following required field(s) were not provided: ";
                string fields = string.Empty;
                for (int i = 0; i < _requiredFieldsNotProvided.Length; i++)
                {
                    if (i == 0)
                    {
                        fields = _requiredFieldsNotProvided[i];
                    }
                    else
                    {
                        fields += ", " + _requiredFieldsNotProvided[i];
                    }
                }
                msg += fields;
                return msg;
            }
        }
    }
}
